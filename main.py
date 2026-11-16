import html
import io
import logging
import os
import signal
import socket
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone

import docker
from docker.errors import DockerException
from dotenv import load_dotenv
import requests
import schedule

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("docker-logger")

# Конфигурация из переменных окружения
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_THREAD_ID = os.getenv("TELEGRAM_THREAD_ID", "").strip()
SERVER_NAME = os.getenv("SERVER_NAME", socket.gethostname()).strip()

SEND_TIME = os.getenv("SEND_TIME", "09:00").strip()
LOG_HOURS = float(os.getenv("LOG_HOURS", "24"))
LOG_TAIL = os.getenv("LOG_TAIL", "all").strip()  # "all" или число строк, например "2000"
SEND_MODE = os.getenv("SEND_MODE", "files").strip().lower()  # "files", "zip", "text"
RUN_ON_STARTUP = os.getenv("RUN_ON_STARTUP", "true").strip().lower() in ("1", "true", "yes")
EXCLUDE_SELF = os.getenv("EXCLUDE_SELF", "true").strip().lower() in ("1", "true", "yes")
EXCLUDE_CONTAINERS = {
    name.strip()
    for name in os.getenv("EXCLUDE_CONTAINERS", "").split(",")
    if name.strip()
}

# Лимит Telegram Bot API на отправку файла — 50 МБ (берем с запасом 48 МБ)
MAX_FILE_SIZE_BYTES = 48 * 1024 * 1024
MAX_TEXT_LENGTH = 3900

stop_requested = False


def handle_signal(signum, _frame):
    global stop_requested
    logger.info("Получен сигнал завершения (%s). Остановка...", signum)
    stop_requested = True


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def telegram_request(method: str, data: dict, files: dict | None = None) -> bool:
    """Отправка запроса к Telegram Bot API с обработкой 429 Too Many Requests."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    if TELEGRAM_THREAD_ID:
        data["message_thread_id"] = TELEGRAM_THREAD_ID

    for attempt in range(3):
        try:
            response = requests.post(url, data=data, files=files, timeout=60)
            if response.status_code == 200:
                return True
            if response.status_code == 429:
                retry_after = response.json().get("parameters", {}).get("retry_after", 5)
                logger.warning("Превышен лимит запросов Telegram. Ожидание %s сек...", retry_after)
                time.sleep(retry_after + 1)
                continue
            logger.error(
                "Ошибка Telegram API (%s): %s - %s",
                method,
                response.status_code,
                response.text,
            )
            return False
        except requests.RequestException as exc:
            logger.error("Ошибка сети при обращении к Telegram API (попытка %s/3): %s", attempt + 1, exc)
            time.sleep(3)
    return False


def send_telegram_message(text: str) -> bool:
    """Отправка текстового сообщения в Telegram."""
    return telegram_request(
        "sendMessage",
        {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
    )


def send_telegram_document(filename: str, content_bytes: bytes, caption: str = "") -> bool:
    """Отправка файла в Telegram с автоматическим сжатием, если размер превышает лимит."""
    if not content_bytes:
        content_bytes = "[Нет записей за указанный период]\n".encode("utf-8")

    # Если файл больше 48 МБ, сжимаем его в ZIP
    if len(content_bytes) > MAX_FILE_SIZE_BYTES and not filename.endswith(".zip"):
        logger.info("Файл %s слишком большой (%d байт), сжимаем в ZIP...", filename, len(content_bytes))
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(filename, content_bytes)
        content_bytes = zip_buffer.getvalue()
        filename = f"{filename}.zip"

    # Если даже после сжатия больше 48 МБ — обрезаем хвост
    if len(content_bytes) > MAX_FILE_SIZE_BYTES:
        logger.warning("Файл %s превышает лимит Telegram даже после сжатия, обрезаем...", filename)
        content_bytes = content_bytes[-MAX_FILE_SIZE_BYTES:]

    data = {"chat_id": TELEGRAM_CHAT_ID}
    if caption:
        data["caption"] = caption[:1024]
        data["parse_mode"] = "HTML"

    files = {"document": (filename, io.BytesIO(content_bytes))}
    return telegram_request("sendDocument", data=data, files=files)


def collect_container_logs() -> tuple[list[dict], datetime, datetime]:
    """Собирает логи всех запущенных контейнеров."""
    client = docker.from_env()
    now_utc = datetime.now(timezone.utc)
    since_utc = now_utc - timedelta(hours=LOG_HOURS) if LOG_HOURS > 0 else None

    tail_arg: int | str = "all"
    if LOG_TAIL.isdigit() and int(LOG_TAIL) > 0:
        tail_arg = int(LOG_TAIL)

    containers = client.containers.list(filters={"status": "running"})
    current_hostname = socket.gethostname()

    results = []
    for container in sorted(containers, key=lambda c: c.name):
        if EXCLUDE_SELF and (
            container.id.startswith(current_hostname)
            or container.name == "docker-server-logger"
        ):
            continue

        if container.name in EXCLUDE_CONTAINERS:
            logger.info("Пропуск контейнера %s (в списке исключений)", container.name)
            continue

        try:
            log_kwargs = {
                "stdout": True,
                "stderr": True,
                "timestamps": True,
                "tail": tail_arg,
            }
            if since_utc is not None:
                log_kwargs["since"] = since_utc

            raw_logs = container.logs(**log_kwargs)
            decoded_logs = raw_logs.decode("utf-8", errors="replace").strip()

            image_tags = container.image.tags
            image_name = image_tags[0] if image_tags else container.image.short_id

            results.append(
                {
                    "name": container.name,
                    "short_id": container.short_id,
                    "image": image_name,
                    "status": container.status,
                    "logs": decoded_logs,
                    "lines_count": len(decoded_logs.splitlines()) if decoded_logs else 0,
                }
            )
        except Exception as exc:
            logger.error("Ошибка при чтении логов контейнера %s: %s", container.name, exc)
            results.append(
                {
                    "name": container.name,
                    "short_id": container.short_id,
                    "image": "unknown",
                    "status": container.status,
                    "logs": f"Ошибка получения логов: {exc}",
                    "lines_count": 1,
                }
            )

    return results, since_utc or now_utc, now_utc


def build_summary_message(containers_data: list[dict], now_local: datetime) -> str:
    """Формирует сводное сообщение о состоянии контейнеров."""
    period_str = f"за последние {LOG_HOURS:g} ч." if LOG_HOURS > 0 else "за всё время"
    lines = [
        f"📋 <b>Ежедневный отчёт по логам Docker</b>",
        f"🖥 <b>Сервер:</b> <code>{html.escape(SERVER_NAME)}</code>",
        f"🕒 <b>Время:</b> <code>{now_local.strftime('%Y-%m-%d %H:%M:%S')}</code>",
        f"⏳ <b>Период:</b> {period_str}",
        f"📦 <b>Запущено контейнеров:</b> {len(containers_data)}",
        "",
    ]

    if not containers_data:
        lines.append("ℹ️ Активных контейнеров не найдено.")
    else:
        for item in containers_data:
            name = html.escape(item["name"])
            image = html.escape(item["image"])
            count = item["lines_count"]
            icon = "🟢" if count > 0 else "⚪️"
            lines.append(f"{icon} <b>{name}</b> (<code>{image}</code>) — {count} стр.")

    return "\n".join(lines)


def send_daily_logs():
    """Основная задача сбора и отправки логов в Telegram."""
    logger.info("Запуск задачи сбора логов контейнеров...")
    now_local = datetime.now()
    date_tag = now_local.strftime("%Y-%m-%d_%H-%M")

    try:
        containers_data, _, _ = collect_container_logs()
    except DockerException as exc:
        error_msg = (
            f"❌ <b>Ошибка подключения к Docker Daemon на сервере "
            f"<code>{html.escape(SERVER_NAME)}</code>:</b>\n"
            f"<pre>{html.escape(str(exc))}</pre>"
        )
        logger.error("Не удалось подключиться к Docker: %s", exc)
        send_telegram_message(error_msg)
        return

    summary_text = build_summary_message(containers_data, now_local)
    send_telegram_message(summary_text)

    if not containers_data:
        logger.info("Нет контейнеров для отправки логов.")
        return

    if SEND_MODE == "zip":
        # Отправляем один общий ZIP-архив со всеми логами
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for item in containers_data:
                content = item["logs"] or "[Нет новых записей в логах за указанный период]\n"
                zf.writestr(f"{item['name']}_{date_tag}.log", content.encode("utf-8"))

        archive_name = f"docker_logs_{SERVER_NAME}_{date_tag}.zip"
        caption = f"📦 Архив логов всех контейнеров ({len(containers_data)} шт.)"
        send_telegram_document(archive_name, zip_buffer.getvalue(), caption=caption)

    elif SEND_MODE == "text":
        # Отправляем логи текстом прямо в чат (с разбивкой по лимиту символов Telegram)
        for item in containers_data:
            header = f"📄 <b>{html.escape(item['name'])}</b> (<code>{html.escape(item['short_id'])}</code>)\n"
            log_text = item["logs"] or "[Нет новых записей за указанный период]"
            if len(log_text) > MAX_TEXT_LENGTH:
                log_text = "...(обрезано)...\n" + log_text[-MAX_TEXT_LENGTH:]
            msg = f"{header}<pre>{html.escape(log_text)}</pre>"
            send_telegram_message(msg)
            time.sleep(0.5)

    else:
        # Режим по умолчанию ("files"): отдельный .log файл на каждый контейнер
        for item in containers_data:
            content = item["logs"] or "[Нет новых записей в логах за указанный период]\n"
            filename = f"{item['name']}_{date_tag}.log"
            caption = (
                f"📄 <b>{html.escape(item['name'])}</b> "
                f"(<code>{html.escape(item['short_id'])}</code>) — {item['lines_count']} стр."
            )
            send_telegram_document(filename, content.encode("utf-8"), caption=caption)
            time.sleep(0.5)

    logger.info("Отправка логов успешно завершена (обработано контейнеров: %d).", len(containers_data))


def validate_config() -> bool:
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "your_bot_token_here":
        logger.error("Не задан TELEGRAM_BOT_TOKEN! Укажите токен бота в файле .env")
        return False
    if not TELEGRAM_CHAT_ID or TELEGRAM_CHAT_ID == "your_chat_id_here":
        logger.error("Не задан TELEGRAM_CHAT_ID! Укажите ID чата/канала в файле .env")
        return False
    return True


def main():
    if hasattr(time, "tzset"):
        time.tzset()

    if not validate_config():
        sys.exit(1)

    logger.info("Сервис docker-server-logger запущен.")
    logger.info(
        "Настройки: Сервер=%s | Время отправки=%s | Период=%s ч. | Режим=%s",
        SERVER_NAME,
        SEND_TIME,
        LOG_HOURS,
        SEND_MODE,
    )

    if RUN_ON_STARTUP:
        logger.info("RUN_ON_STARTUP=true -> Выполняем первичную отправку логов при старте...")
        send_daily_logs()

    schedule.every().day.at(SEND_TIME).do(send_daily_logs)
    logger.info("Расписание установлено: каждый день в %s (текущее время: %s)", SEND_TIME, datetime.now().strftime("%H:%M:%S"))

    while not stop_requested:
        schedule.run_pending()
        time.sleep(1)

    logger.info("Сервис остановлен.")


if __name__ == "__main__":
    main()
