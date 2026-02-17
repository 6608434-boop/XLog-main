"""
Менеджер профилей XLog — работа с файлами профилей на Яндекс.Диске
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

from .yadisk_client import YandexDiskClient
from .logger import logger

# Типы для удобства
ProfileDict = Dict[str, Any]
FilesDict = Dict[str, Optional[str]]


class ProfileManager:
    """Управляет профилями и их файлами на Яндекс.Диске"""

    # Список файлов, которые должны быть в каждом профиле
    PROFILE_FILES = ["key.docx", "king.docx", "rules.docx", "library.docx", "welcome.docx"]

    def __init__(self, disk_client: YandexDiskClient, config: Dict[str, Any]):
        """
        Инициализация менеджера профилей.

        Args:
            disk_client: Клиент для работы с Яндекс.Диском
            config: Конфигурация с информацией о профилях
        """
        self.disk = disk_client
        self.config = config
        self.profiles = config.get("profiles", [])
        logger.info(f"ProfileManager initialized with {len(self.profiles)} profiles")

    def get_all_profiles(self) -> List[ProfileDict]:
        """Возвращает список всех доступных профилей"""
        return self.profiles

    def get_profile_files(self, profile_name: str) -> FilesDict:
        """
        Читает ВСЕ файлы профиля с Яндекс.Диска.

        Returns:
            Словарь с содержимым файлов: key, king, rules, library, welcome
        """
        files = {}

        for file_name in self.PROFILE_FILES:
            key = file_name.replace('.docx', '')
            try:
                path = f"{profile_name}/{file_name}"
                content = self.disk.read_file(path)

                if content:
                    files[key] = content
                    logger.debug(f"Loaded {file_name}: {len(content)} chars")
                else:
                    files[key] = ""
                    logger.warning(f"File {file_name} is empty")

            except Exception as e:
                logger.error(f"Failed to read {file_name}: {e}")
                files[key] = ""

        loaded = [k for k, v in files.items() if v]
        empty = [k for k, v in files.items() if not v]
        logger.info(f"Profile {profile_name}: loaded {loaded}, empty {empty}")

        return files

    def save_profile_file(self, profile_name: str, file_key: str, content: str) -> bool:
        """
        Сохраняет содержимое в файл профиля.

        Args:
            profile_name: Имя профиля
            file_key: Ключ файла (king, rules, library, welcome)
            content: Текст для сохранения

        Returns:
            True если успешно
        """
        file_name = f"{file_key}.docx"
        path = f"{profile_name}/{file_name}"
        return self.disk.write_file(path, content)

    def build_context(self, profile_name: str, limit: int = 10) -> str:
        """
        Собирает полный контекст для DeepSeek:
        - king.docx (личность)
        - rules.docx (правила)
        - library.docx (опыт/знания)
        - последние N сообщений из логов
        """
        files = self.get_profile_files(profile_name)
        parts = []

        if files.get('king'):
            parts.append(f"ТЫ — ЛИЧНОСТЬ:\n{files['king']}\n")
        if files.get('rules'):
            parts.append(f"ПРАВИЛА ОБЩЕНИЯ:\n{files['rules']}\n")
        if files.get('library'):
            parts.append(f"ТВОИ ЗНАНИЯ И ОПЫТ:\n{files['library']}\n")

        recent = self.get_recent_messages(profile_name, limit)
        if recent:
            parts.append(f"ПОСЛЕДНИЕ СООБЩЕНИЯ В ЧАТЕ:\n{recent}\n")

        return "\n".join(parts)

    def save_message(self, profile_name: str, role: str, text: str, timestamp: datetime):
        """Сохраняет сообщение в лог профиля (оставляем как .txt логи)"""
        try:
            date_path = timestamp.strftime("%Y/%m/%d")
            log_path = f"{profile_name}/logs/{date_path}/log.txt"

            folder_path = f"{profile_name}/logs/{date_path}"
            self.disk.ensure_folder_exists(folder_path)

            time_str = timestamp.strftime("%H:%M:%S")
            log_entry = f"[{time_str}] {role}: {text}\n"

            success = self.disk._write_text_file(log_path, log_entry, append=True)

            if success:
                logger.debug(f"Message saved to {log_path}")
            else:
                logger.error(f"Failed to save message to {log_path}")

        except Exception as e:
            logger.error(f"Failed to save message: {e}")

    def get_recent_messages(self, profile_name: str, limit: int = 10) -> str:
        """Читает последние сообщения из лога профиля"""
        today = datetime.now()
        yesterday = datetime.now().replace(day=today.day - 1)

        date_path = today.strftime("%Y/%m/%d")
        log_path = f"{profile_name}/logs/{date_path}/log.txt"
        content = self.disk._read_text_file(log_path)

        if not content:
            date_path = yesterday.strftime("%Y/%m/%d")
            log_path = f"{profile_name}/logs/{date_path}/log.txt"
            content = self.disk._read_text_file(log_path)

        if not content:
            return ""

        lines = content.strip().split('\n')
        last_lines = lines[-limit:] if len(lines) > limit else lines
        return '\n'.join(last_lines)

    def add_to_library(self, profile_name: str, text: str) -> bool:
        """Добавляет текст в library.docx профиля"""
        try:
            path = f"{profile_name}/library.docx"
            current = self.read_profile_file(profile_name, "library")

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            new_content = f"{current}\n\n[{timestamp}] ДОБАВЛЕНО:\n{text}" if current else text

            return self.save_profile_file(profile_name, "library", new_content)

        except Exception as e:
            logger.error(f"Error adding to library: {e}")
            return False

    def read_profile_file(self, profile_name: str, file_key: str) -> Optional[str]:
        """Читает конкретный файл профиля"""
        try:
            files = self.get_profile_files(profile_name)
            return files.get(file_key)
        except Exception as e:
            logger.error(f"Error reading {file_key} for {profile_name}: {e}")
            return None