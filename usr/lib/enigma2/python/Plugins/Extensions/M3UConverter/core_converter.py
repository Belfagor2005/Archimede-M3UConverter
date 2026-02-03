# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

"""
#########################################################
#                                                       #
#  Archimede Universal Converter Plugin                 #
#  Version: 3.0                                         #
#  Created by Lululla (https://github.com/Belfagor2005) #
#  License: CC BY-NC-SA 4.0                             #
#  https://creativecommons.org/licenses/by-nc-sa/4.0    #
#  Last Modified: "20:05 - 20251102"                    #
#                                                       #
#  Credits:                                             #
#  - Original concept by Lululla                        #
#  Usage of this code without proper attribution        #
#  is strictly prohibited.                              #
#  For modifications and redistribution,                #
#  please maintain this credit header.                  #
#########################################################
"""
__author__ = "Lululla"

# ======================== IMPORTS ========================
# 🧠 STANDARD LIBRARIES (Python built-ins)
import shutil
import hashlib
import unicodedata
from re import sub
from time import strftime
from threading import Lock
from collections import defaultdict
from os.path import exists, isdir, join, basename
from os import access, W_OK, listdir, remove, replace, chmod, system, makedirs

# 🧩 ENIGMA2 COMPONENTS
from Components.config import config


# 🧱 LOCAL MODULES
from .constants import (
    LOG_DIR,
    DB_PATCH,
    BASE_STORAGE_PATH,
    ARCHIMEDE_CONVERTER_PATH,
)
from .utils import (
    clean_group_name,
    transliterate_text,
    create_bouquets_backup
)
from .Logger_clr import get_logger


# ==================== LOGGER ====================
logger = get_logger(
    log_path=LOG_DIR,
    plugin_name="M3U_CONVERTER",
    clear_on_start=True,
    max_size_mb=0.5
)


class CoreConverter:
    """Main converter class with backup and logging functionality."""

    _instance = None
    _lock = Lock()

    def __new__(cls):
        """Singleton pattern implementation."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance.__initialized = False
        return cls._instance

    def __init__(self):
        """Initialize core converter with directories and logging."""
        if not self.__initialized:
            self.backup_dir = join(
                ARCHIMEDE_CONVERTER_PATH,
                "archimede_backup")
            self.log_file = join(
                ARCHIMEDE_CONVERTER_PATH,
                "core_converter_archimede_converter.log")
            self._create_necessary_directories()
            self.__initialized = True

    def _log_current_configuration(self):
        """Log the current configuration in setup.xml order"""
        try:
            logger.info("=== CURRENT CONFIGURATION (Setup Order) ===")
            logger.info("📁 FILE & STORAGE SETTINGS:")
            logger.info(
                f"  • Default Folder: {
                    config.plugins.m3uconverter.lastdir.value}")
            logger.info(
                f"  • Large file threshold: {
                    config.plugins.m3uconverter.large_file_threshold_mb.value} MB")

            logger.info("🎯 BOUQUET SETTINGS:")
            logger.info(
                f"  • Bouquet Mode: {
                    config.plugins.m3uconverter.bouquet_mode.value}")
            logger.info(
                f"  • Bouquet Position: {
                    config.plugins.m3uconverter.bouquet_position.value}")

            logger.info("🔧 STREAM & CONVERSION:")
            logger.info(
                f"  • Convert HLS Streams: {
                    config.plugins.m3uconverter.hls_convert.value}")

            logger.info("⚙️ SYSTEM SETTINGS:")
            logger.info(
                f"  • Create Backup: {
                    config.plugins.m3uconverter.backup_enable.value}")
            logger.info(
                f"  • Max Backups: {
                    config.plugins.m3uconverter.max_backups.value}")
            logger.info(
                f"  • Debug Mode: {
                    config.plugins.m3uconverter.enable_debug.value}")

            logger.info("📡 EPG SETTINGS:")
            logger.info(
                f"  • Enable EPG: {
                    config.plugins.m3uconverter.epg_enabled.value}")

            logger.info("  📊 EPG CONFIGURATION:")
            logger.info(
                f"    • EPG Language: {
                    config.plugins.m3uconverter.language.value}")
            logger.info(
                f"    • EPG Generation Mode: {
                    config.plugins.m3uconverter.epg_generation_mode.value}")
            logger.info(
                f"    • Database Mode: {
                    config.plugins.m3uconverter.epg_database_mode.value}")
            logger.info(
                f"    • Use Manual Database: {
                    config.plugins.m3uconverter.use_manual_database.value}")
            logger.info(
                f"    • Ignore DVB-T services: {config.plugins.m3uconverter.ignore_dvbt.value}")

            logger.info("  🎯 SIMILARITY THRESHOLDS:")
            logger.info(
                f"    • Global Similarity: {
                    config.plugins.m3uconverter.similarity_threshold.value}%")
            logger.info(
                f"    • Rytec Similarity: {
                    config.plugins.m3uconverter.similarity_threshold_rytec.value}%")
            logger.info(
                f"    • DVB Similarity: {
                    config.plugins.m3uconverter.similarity_threshold_dvb.value}%")

            logger.info("  💾 MANUAL DATABASE:")
            logger.info(
                f"    • Manual DB Max Size: {
                    config.plugins.m3uconverter.manual_db_max_size.value}")
            logger.info(
                f"    • Auto-open Editor: {config.plugins.m3uconverter.auto_open_editor.value}")

            logger.info("  🗄️ DEBUG STORAGE:")
            logger.info(f"    • BASE_STORAGE_PATH: {BASE_STORAGE_PATH}")
            logger.info(
                f"    • ARCHIMEDE_CONVERTER_PATH: {ARCHIMEDE_CONVERTER_PATH}")
            logger.info(f"    • LOG_DIR: {LOG_DIR}")
            logger.info(f"    • DB PATCH: {DB_PATCH}")
            logger.info(f"    • USB exists: {isdir('/media/usb/')}")
            logger.info(f"    • USB writable: {access('/media/usb/', W_OK)}")

            logger.info("=== END CONFIGURATION ===")

        except Exception as e:
            logger.error(f"Error logging configuration: {str(e)}")

    def _create_necessary_directories(self):
        """Create necessary directories if they don't exist."""
        try:
            makedirs(self.backup_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"Error creating directories: {str(e)}")

    def get_safe_filename(self, name):
        """Generate a secure file name for bouquets with duplicate suffixes."""
        # Remove known suffixes
        for suffix in ['_m3ubouquet', '_bouquet', '_iptv', '_tv']:
            if name.endswith(suffix):
                name = name[:-len(suffix)]

        # Normalize and convert to ASCII
        normalized = unicodedata.normalize("NFKD", name)
        safe_name = normalized.encode('ascii', 'ignore').decode('ascii')

        # Replace invalid characters with underscores
        safe_name = sub(r'[^a-zA-Z0-9_-]', '_', safe_name)
        safe_name = sub(r'_+', '_', safe_name).strip('_')

        suffix = "_m3ubouquet"
        base_name = safe_name[:50 - len(suffix)] if len(
            safe_name) > 50 - len(suffix) else safe_name

        return base_name + suffix if base_name else "m3uconverter_bouquet"

    def remove_suffixes(self, name):
        """Remove all known suffixes from the name for display purposes."""
        suffixes = ['_m3ubouquet', '_bouquet', '_iptv', '_tv']

        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break

        return name

    def write_group_bouquet(self, safe_name, channels, epg_mapper=None):
        """Write bouquet using bouquet_sref for IPTV and sref as fallback"""
        try:
            bouquet_dir = "/etc/enigma2"
            filename = join(bouquet_dir, "userbouquet." + safe_name + ".tv")
            temp_file = filename + ".tmp"

            if not exists(bouquet_dir):
                makedirs(bouquet_dir, exist_ok=True)

            name_bouquet = clean_group_name(self.remove_suffixes(safe_name))

            with open(temp_file, "w", encoding="utf-8", buffering=65536) as f:
                f.write(f"#NAME {name_bouquet}\n")
                f.write(
                    "#SERVICE 1:64:0:0:0:0:0:0:0:0::--- | Archimede Converter | ---\n")
                f.write("#DESCRIPTION --- | Archimede Converter | ---\n")

                for ch in channels:
                    if not ch.get('url') or len(ch['url']) < 10:
                        continue

                    service_ref = ch.get('bouquet_sref')
                    if not service_ref:
                        service_ref = ch.get('sref', '')  # Fallback a sref

                    if not service_ref:
                        if epg_mapper:
                            service_ref = epg_mapper._generate_service_reference(
                                ch['url'])
                        else:
                            service_ref = self._generate_basic_service_reference(
                                ch['url'])

                    f.write(f"#SERVICE {service_ref}\n")

                    # Clean name for description
                    desc = ch.get('name', 'Unknown Channel')
                    desc = ''.join(
                        c for c in desc if c.isprintable() or c.isspace())
                    desc = transliterate_text(desc)
                    f.write(f"#DESCRIPTION {desc}\n")

            # Replace file
            if exists(filename):
                remove(filename)
            replace(temp_file, filename)
            chmod(filename, 0o644)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(
                    f"✅ Manual bouquet written: {safe_name} with {
                        len(channels)} channels")
            return True

        except Exception as e:
            if exists(temp_file):
                try:
                    remove(temp_file)
                except Exception:
                    pass
            logger.error(
                f"❌ Failed to write manual bouquet {safe_name}: {
                    str(e)}")
            return False

    def _generate_basic_service_reference(self, url):
        """Basic fallback - to be used ONLY if epg_mapper is not available"""
        if not url:
            return None

        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:8]
        service_id = int(url_hash, 16) % 65536
        encoded_url = url.replace(':', '%3a').replace(' ', '%20')
        return f"4097:0:1:{service_id}:0:0:0:0:0:0:{encoded_url}"

    def update_main_bouquet(self, groups):
        """Update the main bouquet file with generated group bouquets."""
        main_file = "/etc/enigma2/bouquets.tv"
        # Read existing content
        existing_lines = []
        if exists(main_file):
            try:
                with open(main_file, "r", encoding="utf-8") as f:
                    existing_lines = f.readlines()
            except Exception as e:
                logger.error(f"Error reading bouquets.tv: {str(e)}")
                return False

        if config.plugins.m3uconverter.backup_enable.value:
            create_bouquets_backup()

        # Create new lines to add
        new_lines = []
        for group in groups:
            safe_name = self.get_safe_filename(group)
            bouquet_path = "userbouquet." + safe_name + ".tv"
            line_to_add = '#SERVICE 1:7:1:0:0:0:0:0:0:0:FROM BOUQUET "' + \
                bouquet_path + '" ORDER BY bouquet\n'

            # Check if line already exists
            if not any(line_to_add in line for line in existing_lines):
                new_lines.append(line_to_add)

        if not new_lines:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("No new bouquets to add")
            return True

        # Add new lines in correct position
        if config.plugins.m3uconverter.bouquet_position.value == "top":
            final_content = new_lines + existing_lines
        else:
            final_content = existing_lines + new_lines

        # Write file
        try:
            with open(main_file, "w", encoding="utf-8") as f:
                f.writelines(final_content)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(
                    f"Updated bouquets.tv with {
                        len(new_lines)} new bouquets")
            return True
        except Exception as e:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error(f"Error writing bouquets.tv: {str(e)}")
            return False

    def safe_conversion(self, function, *args, **kwargs):
        """Perform conversion with automatic backup and error handling."""
        try:
            self._create_backup()
            result = function(*args, **kwargs)
            self._log_success(function.__name__)
            return result
        except Exception as e:
            self._log_error(e)
            self._restore_backup()
            raise RuntimeError(
                f"Conversion failed (restored backup). Error: {
                    str(e)}")

    def _create_backup(self):
        """Create a backup of the existing bouquets."""
        try:
            if not exists("/etc/enigma2/bouquets.tv"):
                return

            self.cleanup_old_backups(
                config.plugins.m3uconverter.max_backups.value)

            timestamp = strftime("%Y%m%d_%H%M%S")
            import random
            unique_id = random.randint(100, 999)
            backup_file = join(
                self.backup_dir,
                f"bouquets_{timestamp}_{unique_id}.tv")
            shutil.copy2("/etc/enigma2/bouquets.tv", backup_file)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"💾 Backup created: {basename(backup_file)}")

        except Exception as e:
            raise RuntimeError(f"Backup failed: {str(e)}")

    def _restore_backup(self):
        """Restore the most recent available backup."""
        try:
            backups = sorted([f for f in listdir(self.backup_dir) if f.startswith(
                "bouquets_") and f.endswith(".tv")])

            if backups:
                latest_backup = join(self.backup_dir, backups[-1])
                shutil.copy2(latest_backup, "/etc/enigma2/bouquets.tv")
        except Exception as e:
            raise RuntimeError(f"Restore failed: {str(e)}")

    def _log_success(self, operation_name):
        """Log a successful operation."""
        message = f"{strftime('%Y-%m-%d %H:%M:%S')} [SUCCESS] {operation_name}"
        self._write_to_log(message)

    def _log_error(self, error):
        """Log an error."""
        message = f"{strftime('%Y-%m-%d %H:%M:%S')} [ERROR] {str(error)}"
        self._write_to_log(message)

    def _write_to_log(self, message):
        """Write message to log file."""
        try:
            with open(self.log_file, "a") as f:
                f.write(message + "\n")
        except Exception:
            print(f"Fallback log: {message}")

    def _is_url_accessible(self, url, timeout=5):
        """Check if a URL is reachable."""
        if not url:
            return False

        try:
            cmd = f"curl --max-time {timeout} --head --silent --fail --output /dev/null {url}"
            return system(cmd) == 0
        except Exception:
            return False

    def cleanup_old_backups(self, max_backups=3):
        """Keep only the latest N backups."""
        try:
            backups = sorted([f for f in listdir(self.backup_dir) if f.startswith(
                "bouquets_") and f.endswith(".tv")])

            for old_backup in backups[:-max_backups]:
                remove(join(self.backup_dir, old_backup))
        except Exception as e:
            self._log_error(f"Cleanup failed: {str(e)}")


class UnifiedChannelMapping:
    """Unified channel mapping structure to replace multiple redundant maps."""

    def __init__(self):
        """Initialize unified channel mapping with empty structures."""
        # Rytec databases
        self.rytec = {
            'basic': {},                    # Base Rytec mapping (id -> sref)
            # Clean names mapping (clean_name -> sref)
            'clean': {},
            # Extended info with variants (id -> [variants])
            'extended': defaultdict(list),
            'by_name': defaultdict(list)    # Rytec entries by channel name
        }

        # DVB databases
        # DVB channels from lamedb/bouquets (name -> [services])
        self.dvb = defaultdict(list)

        # Optimized structures
        # Optimized for matching (name -> best_service)
        self.optimized = {}
        # Reverse mapping (channel_id -> satellite)
        self.reverse_mapping = {}
        # Auto-discovered references (channel_id -> sref)
        self.auto_discovered = {}

        # Caches
        self._clean_name_cache = {}         # Cache for cleaned names
        self._clean_cache_max_size = 10000  # Cache max size

    def clear(self):
        """Clear all mappings."""
        self.rytec['basic'].clear()
        self.rytec['clean'].clear()
        self.rytec['extended'].clear()
        self.rytec['by_name'].clear()
        self.dvb.clear()
        self.optimized.clear()
        self.reverse_mapping.clear()
        self.auto_discovered.clear()
        self._clean_name_cache.clear()
