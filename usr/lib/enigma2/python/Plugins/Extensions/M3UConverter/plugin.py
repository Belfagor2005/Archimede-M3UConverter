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
import json
import time
import glob
import shutil
import codecs
import hashlib
import threading
import subprocess
from time import strftime
from threading import Lock
from urllib.parse import unquote
from collections import defaultdict
from os import access, W_OK, listdir, remove, replace, chmod, mkdir, makedirs
from re import compile, sub, findall, DOTALL, MULTILINE, IGNORECASE, search, escape
from os.path import exists, isdir, isfile, join, normpath, basename, dirname, getsize, getmtime

# ⚡ TWISTED / ASYNC
from twisted.internet import threads
from twisted.internet.reactor import callInThread, callFromThread

# 📺 ENIGMA2 CORE
from enigma import eServiceReference, eTimer, eDVBDB

# 🧩 ENIGMA2 COMPONENTS
from Components.Label import Label
from Components.MenuList import MenuList
from Components.ActionMap import ActionMap
from Components.Sources.Progress import Progress
from Components.Sources.StaticText import StaticText
from Components.config import (
    config, ConfigSelection, ConfigSubsection, ConfigYesNo,
    ConfigNumber, ConfigSelectionNumber
)

# 🪟 ENIGMA2 SCREENS
from Screens.Screen import Screen
from Screens.ChoiceBox import ChoiceBox
from Screens.MessageBox import MessageBox

# 🧰 ENIGMA2 TOOLS
from Tools.Directories import fileExists

# 🧱 LOCAL MODULES
from . import _
from .version import CURRENT_VERSION
from .Logger_clr import get_logger
from .constants import (
    M3UConverterSettings,
    SCREEN_WIDTH,
    PLUGIN_TITLE,
    PLUGIN_PATH,
    ARCHIMEDE_CONVERTER_PATH,
    ARCHIMEDE_M3U_PATH,
    LOG_DIR,
    DEBUG_DIR,
    DB_PATCH,
    LANGUAGE_TO_COUNTRY
)
from .utils import (
    AspectManager,
    M3UFileBrowser,
    _reload_services_after_delay,
    create_bouquets_backup,
    clean_group_name,
    update_mounts_configuration,
    default_movie_path
)
from .plugin_info import PluginInfoScreen
from .core_converter import CoreConverter, UnifiedChannelMapping

# Try to import lxml, install if not available
try:
    from lxml import etree
    LXML_AVAILABLE = True
except ImportError:
    LXML_AVAILABLE = False
    try:
        subprocess.run(["pip3", "install", "lxml"], check=True, capture_output=True, text=True)
        from lxml import etree
        LXML_AVAILABLE = True
    except Exception as e:
        print(e)
        LXML_AVAILABLE = False
        import xml.etree.ElementTree as ET

# ==================== CONSTANTS & PATHS ====================
ICON_STORAGE = 0
ICON_PARENT = 1
ICON_CURRENT = 2


# Make directory
try:
    makedirs(ARCHIMEDE_CONVERTER_PATH, exist_ok=True)
    makedirs(DEBUG_DIR, exist_ok=True)
    print(f"📁 Directories ready: {ARCHIMEDE_CONVERTER_PATH}")

    makedirs(ARCHIMEDE_M3U_PATH, exist_ok=True)
    makedirs(DEBUG_DIR, exist_ok=True)
    print(f"📁 Directories ready: {ARCHIMEDE_M3U_PATH}")
except Exception as e:
    print(f"Error creating directories: {e}")


# ==================== LOGGER ====================
logger = get_logger(
    log_path=LOG_DIR,
    plugin_name="M3U_CONVERTER",
    clear_on_start=True,
    max_size_mb=0.5
)

# ==================== CONFIG INITIALIZATION ====================
config.plugins.m3uconverter = ConfigSubsection()

# File and Storage Settings
default_dir = config.movielist.last_videodir.value if isdir(config.movielist.last_videodir.value) else default_movie_path()
config.plugins.m3uconverter.lastdir = ConfigSelection(default=default_dir, choices=[])
config.plugins.m3uconverter.large_file_threshold_mb = ConfigSelectionNumber(default=10, stepwidth=5, min=1, max=50)

# Bouquet Settings
config.plugins.m3uconverter.bouquet_mode = ConfigSelection(
    default="single",
    choices=[("single", _("Single Bouquet")), ("multi", _("Multiple Bouquets"))]
)

config.plugins.m3uconverter.bouquet_position = ConfigSelection(
    default="bottom",
    choices=[("top", _("Top")), ("bottom", _("Bottom"))]
)

# Stream Conversion Settings
config.plugins.m3uconverter.hls_convert = ConfigYesNo(default=True)

# System and Performance Settings
config.plugins.m3uconverter.backup_enable = ConfigYesNo(default=True)
config.plugins.m3uconverter.max_backups = ConfigNumber(default=3)
config.plugins.m3uconverter.enable_debug = ConfigYesNo(default=False)

# EPG Settings
config.plugins.m3uconverter.epg_enabled = ConfigYesNo(default=True)

config.plugins.m3uconverter.language = ConfigSelection({
    # Europa
    "it": "Italiano", "en": "English", "de": "Deutsch", "fr": "Français",
    "es": "Español", "pt": "Português", "nl": "Nederlands", "tr": "Türkçe",
    "pl": "Polski", "gr": "Ελληνικά", "cz": "Čeština", "hu": "Magyar",
    "ro": "Română", "se": "Svenska", "no": "Norsk", "dk": "Dansk",
    "fi": "Suomi", "at": "Österreich", "ba": "Bosna", "al": "Shqip",
    "be": "België", "ch": "Schweiz", "cy": "Κύπρος", "hr": "Hrvatski",
    "lt": "Lietuvių", "lv": "Latviešu", "mt": "Malti", "rs": "Српски",
    "sk": "Slovenčina", "bg": "Български",

    # Americhe
    "us": "United States", "usl": "USA Locals", "uss": "USA Sports",
    "ca": "Canada", "mx": "México", "br": "Brasil", "br2": "Brasil 2",
    "cl": "Chile", "co": "Colombia", "cr": "Costa Rica", "do": "República Dominicana",
    "ec": "Ecuador", "pe": "Perú", "uy": "Uruguay", "pa": "Panamá",
    "ar": "Argentina", "jm": "Jamaica",

    # Asia
    "as": "Asian Television", "in": "India", "in2": "India 2", "in4": "India 4",
    "jp": "日本", "jp2": "Japan 2", "kr": "한국", "hk": "香港",
    "id": "Indonesia", "my": "Malaysia", "ph": "Philippines", "ph2": "Philippines 2",
    "th": "ไทย", "vn": "Việt Nam", "pk": "Pakistan", "il": "ישראל",
    "sa": "السعودية", "sg": "Singapore", "mn": "Монгол", "cn": "中国",

    # Oceania
    "au": "Australia", "nz": "New Zealand",

    # Africa - Medio Oriente
    "eg": "مصر", "za": "South Africa", "ng": "Nigeria", "ke": "Kenya",
    "sa1": "Saudi Arabia Alt",

    # Network Speciali
    "bein": "BEIN Sports", "rakuten": "Rakuten TV", "plex": "Plex TV",
    "distro": "Distro TV", "fanduel": "FanDuel", "draftkings": "DraftKings",
    "powertv": "PowerNation", "peacock": "Peacock", "tbnplus": "TBN Plus",
    "thesportplus": "The Sport Plus", "rally": "Rally TV", "sportklub": "Sport Klub",
    "voa": "Voice of America", "aljazeera": "Al Jazeera", "viva": "Viva Russia",

    # Full
    "all": "All Countries - IPTV"
}, default="all")

config.plugins.m3uconverter.epg_generation_mode = ConfigSelection(
    default="epgshare",
    choices=[("epgshare", _("EPGShare Mode")), ("standard", _("Standard Mode"))]
)

config.plugins.m3uconverter.epg_database_mode = ConfigSelection(
    default="dvb",
    choices=[
        ("full", _("DVB + Rytec + DTT (Full)")),
        ("both", _("DVB + Rytec")),
        ("dvb", _("Only DVB")),
        ("rytec", _("Only Rytec")),
        ("dtt", _("Only DTT"))
    ]
)

config.plugins.m3uconverter.ignore_dvbt = ConfigYesNo(default=False)

# Matching and Similarity Settings
config.plugins.m3uconverter.similarity_threshold = ConfigSelectionNumber(default=70, stepwidth=10, min=20, max=100)
config.plugins.m3uconverter.similarity_threshold_rytec = ConfigSelectionNumber(default=70, stepwidth=10, min=20, max=100)
config.plugins.m3uconverter.similarity_threshold_dvb = ConfigSelectionNumber(default=70, stepwidth=10, min=20, max=100)

# Manual Database Settings
config.plugins.m3uconverter.use_manual_database = ConfigYesNo(default=True)
config.plugins.m3uconverter.manual_db_max_size = ConfigNumber(default=1000)
config.plugins.m3uconverter.auto_open_editor = ConfigYesNo(default=False)

# Manual Matches Settings Search Limits
config.plugins.m3uconverter.rytec_search_limit = ConfigSelectionNumber(default=1000, stepwidth=500, min=500, max=20000)
config.plugins.m3uconverter.dvb_search_limit = ConfigSelectionNumber(default=1000, stepwidth=500, min=500, max=20000)

update_mounts_configuration()


# ==================== GLOBAL INSTANCES ====================
aspect_manager = AspectManager()


class EPGServiceMapper:
    """Service mapper for EPG data matching and conversion."""
    def __init__(self, prefer_satellite=True):
        self._match_cache = {}
        self._match_cache_hits = 0
        self._match_cache_misses = 0
        self._incompatible_matches = 0
        self._cache_max_size = 5000

        self.epg_cache = {}
        self.epg_cache_hits = 0
        self.epg_cache_misses = 0

        self._clean_name_cache = {}
        self._channel_cache_hits = 0
        self._channel_cache_misses = 0

        # Add optimization caches
        self._manual_cache = {}

        # Cache cleanup counters
        self._cache_cleanup_counter = 0

        self._rytec_lock = Lock()

        # non utilizzata
        # self.enigma_config = self._load_enigma2_configuration()

        self.country_code = self._get_system_country_code()

        self.similarity_threshold = config.plugins.m3uconverter.similarity_threshold.value / 100.0
        self.similarity_threshold_rytec = config.plugins.m3uconverter.similarity_threshold_rytec.value / 100.0
        self.similarity_threshold_dvb = config.plugins.m3uconverter.similarity_threshold_dvb.value / 100.0

        self.prefer_satellite = prefer_satellite

        # REPLACED: Multiple separate mappings with UnifiedChannelMapping
        self.mapping = UnifiedChannelMapping()
        self.manual_db = ManualDatabaseManager()
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info("✅ Manual database integrated into EPG mapper")

        # Test database accessibility
        try:
            data = self.manual_db.load_database()
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"📁 Manual database accessible: {len(data.get('mappings', []))} mappings")
        except Exception as e:
            logger.error(f"❌ Manual database not accessible: {str(e)}")

        self.database_mode = config.plugins.m3uconverter.epg_database_mode.value
        self.log_file = join(ARCHIMEDE_CONVERTER_PATH, "core_converter_archimede_converter.log")

        # Pre-compiled regex patterns
        self._clean_pattern = compile(r'[^\w\s\-àèéìíòóùúÀÈÉÌÍÒÓÙÚ]', IGNORECASE)
        self._quality_pattern = compile(
            r'\b(4k|uhd|fhd|hd|sd|hq|uhq|sdq|hevc|h265|h264|h\.265|h\.264|full hd|ultra hd|high definition|standard definition|dolby|vision|atmos|avc|mpeg|webdl|webrip|hdtv)\b',
            IGNORECASE
        )

        # Add a separate pattern to preserve the + patterns
        self._plus_pattern = compile(
            r'\+\d+\b|\+HD\b|\+4K\b|\+UHD\b|\+FHD\b|\+HEVC\b|\+H265\b|\+H264\b|\+DV\b|\+ATMOS\b',
            IGNORECASE
        )
        self._stats_counters = {
            'rytec_matches': 0,
            'dvb_matches': 0,
            'dvbt_matches': 0,
            'fallback_matches': 0,
            'manual_db_matches': 0
        }

        # Memory optimization timer
        self.optimize_memory_timer = eTimer()
        self.optimize_memory_timer.callback.append(self._optimize_memory_usage)

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info("EPGServiceMapper initialized with unified mapping")

    def initialize(self):
        """Initialize EPGServiceMapper with database mode control"""
        try:
            logger.info(f"=== INITIALIZATION - DATABASE MODE: {self.database_mode} ===")
            if not hasattr(self, 'services'):
                self.services = []

            # Emergency database repair if needed
            if hasattr(self, 'manual_db'):
                try:
                    # Test database integrity
                    test_data = self.manual_db.load_database()
                    if not test_data or 'mappings' not in test_data:
                        logger.warning("⚠️ Database corrupted, attempting repair...")
                        # This will trigger the enhanced JSON repair
                except Exception as e:
                    logger.error(f"❌ Database integrity check failed: {str(e)}")

            # 1. Load DVB if required (including DVB-T for full/dtt modes)
            if self.database_mode in ["both", "dvb", "full", "dtt"]:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("📥 Loading DVB databases...")
                self._parse_lamedb()
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"✅ Lamedb loaded: {len(self.mapping.dvb)} channels")
                self._parse_existing_bouquets()
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("✅ Existing bouquets loaded")
            else:
                logger.info("⏭️ Skipping DVB databases (mode: rytec only)")

            # 2. Load Rytec if required
            rytec_loaded = False
            if self.database_mode in ["both", "rytec"]:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("🔍 LOADING RYTEC DATABASE...")
                rytec_paths = [
                    "/etc/epgimport/rytec.channels.xml",
                    "/usr/lib/enigma2/python/Plugins/Extensions/EPGImport/rytec.channels.xml",
                ]

                for rytec_path in rytec_paths:
                    if fileExists(rytec_path):
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info(f"📁 Rytec file found: {rytec_path}")
                        self._parse_rytec_channels(rytec_path)
                        rytec_count = len(self.mapping.rytec['basic'])
                        if rytec_count > 0:
                            if config.plugins.m3uconverter.enable_debug.value:
                                logger.info(f"✅ Rytec database loaded: {rytec_count} channels")
                            rytec_loaded = True
                            break

                if not rytec_loaded:
                    logger.warning("⚠️ Rytec database not found or empty")
            else:
                logger.info("⏭️ Skipping Rytec database (mode: dvb only)")

            # 3. Load EPGShare only if in Rytec or Both mode AND rytec not loaded
            if not rytec_loaded and self.database_mode in ["both", "rytec"]:
                if config.plugins.m3uconverter.epg_generation_mode.value == "epgshare":
                    language = config.plugins.m3uconverter.language.value
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"🌐 Downloading EPGShare data for language: {language}")
                    self._download_and_parse_epgshare(language)

            # 4. Fallback only if needed
            if (self.database_mode == "both" and
                    len(self.mapping.rytec['basic']) == 0 and
                    len(self.mapping.dvb) == 0):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.warning("⚠️ No databases loaded! Creating fallback...")
                self._create_fallback_mapping_from_dvb()

            # 5. Optimizations
            self._load_channel_mapping()
            if config.plugins.m3uconverter.ignore_dvbt.value:
                self.services = [s for s in self.services if not self._is_dvb_t_service(s.get('sref', ''))]
            self.optimize_matching()

            # 6. Final statistics
            final_stats = {
                'rytec_basic_count': len(self.mapping.rytec['basic']),
                'rytec_extended_count': len(self.mapping.rytec['extended']),
                'dvb_channels_count': len(self.mapping.dvb),
                'optimized_count': len(self.mapping.optimized)
            }
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"🎯 FINAL DATABASE STATUS: {final_stats}")

            return True

        except Exception as e:
            logger.error(f"❌ Initialization failed: {str(e)}")
            return False

    def _load_channel_mapping(self, mapping_path="/usr/lib/enigma2/python/Plugins/Extensions/M3UConverter/channel_mapping.conf"):
        self.channel_mapping = {}
        self.reverse_channel_mapping = {}  # Reverse map: channel ID -> satellite

        if not fileExists(mapping_path):
            if config.plugins.m3uconverter.enable_debug.value:
                logger.warning("Channel mapping file not found: %s", mapping_path)
            return False

        try:
            current_satellite = None

            with open(mapping_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()

                    if not line or line.startswith('#'):
                        continue

                    # Check for satellite section
                    if line.startswith('[') and line.endswith(']'):
                        current_satellite = line[1:-1].strip()
                        self.channel_mapping[current_satellite] = []
                        continue

                    # Add channel ID to current satellite
                    if current_satellite and line:
                        # Remove any comments after comma
                        channel_id = line.split(',')[0].strip()
                        self.channel_mapping[current_satellite].append(channel_id)

                        # Add to reverse map
                        self.reverse_channel_mapping[channel_id] = current_satellite
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Loaded channel mapping for %d satellites with %d total channels",
                            len(self.channel_mapping), len(self.reverse_channel_mapping))
            return True

        except Exception as e:
            logger.error("Error loading channel mapping: %s", str(e))
            return False

    def _refresh_config(self):
        """Refresh configuration values"""
        old_mode = getattr(self, 'database_mode', 'both')
        self.database_mode = config.plugins.m3uconverter.epg_database_mode.value

        old_similarity = getattr(self, 'similarity_threshold', 0.8)

        # Use ConfigNumber instead of ConfigSelectionNumber
        self.similarity_threshold = config.plugins.m3uconverter.similarity_threshold.value / 100.0
        self.similarity_threshold_rytec = config.plugins.m3uconverter.similarity_threshold_rytec.value / 100.0
        self.similarity_threshold_dvb = config.plugins.m3uconverter.similarity_threshold_dvb.value / 100.0

        if old_similarity != self.similarity_threshold:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"🔄 Similarity thresholds updated - Global: {self.similarity_threshold}, Rytec: {self.similarity_threshold_rytec}, DVB: {self.similarity_threshold_dvb}")

        if old_mode != self.database_mode:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"🔄 Database mode changed from {old_mode} to {self.database_mode} - resetting caches")
            self.reset_caches(clear_match_cache=True)

        logger.info(f"🔄 Config refreshed - Database mode: {self.database_mode}")

    def _optimize_memory_usage(self):
        """Periodic memory cleanup with LRU strategy"""
        try:
            # Initialize caches and counters
            self._init_caches()
            current_size = len(self._match_cache)

            # 1. LRU-based cleanup for match cache
            if current_size > self._cache_max_size:
                self._clean_match_cache_lru()

            # 2. EPG cache cleanup
            self._clean_epg_cache()

            # 3. Periodic deep cleanup
            self._cache_cleanup_counter += 1
            if self._cache_cleanup_counter >= 50:
                self._perform_deep_cleanup()
                self._cache_cleanup_counter = 0

        except Exception as e:
            logger.error(f"Memory optimization error: {str(e)}")

    def _init_caches(self):
        """Initialize all caches if they don't exist"""
        if not hasattr(self, '_name_search_cache'):
            self._name_search_cache = {}
        if not hasattr(self, '_rytec_name_cache'):
            self._rytec_name_cache = {}
        if not hasattr(self, '_manual_cache'):
            self._manual_cache = {}
        if not hasattr(self, '_cache_cleanup_counter'):
            self._cache_cleanup_counter = 0

    def _clean_match_cache_lru(self):
        """Clean match cache using LRU strategy"""
        current_size = len(self._match_cache)
        if current_size > self._cache_max_size:
            # Remove 25% of oldest entries (based on timestamp)
            excess = current_size - int(self._cache_max_size * 0.75)

            # Sort by timestamp to remove oldest entries
            entries_with_times = []
            for key, value in self._match_cache.items():
                if isinstance(value, dict) and 'timestamp' in value:
                    entries_with_times.append((key, value['timestamp']))

            if entries_with_times:
                # Sort by timestamp (oldest first)
                entries_with_times.sort(key=lambda x: x[1])
                keys_to_remove = [key for key, timestamp in entries_with_times[:excess]]

                for key in keys_to_remove:
                    del self._match_cache[key]

                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"🧹 LRU Cleaned {len(keys_to_remove)} oldest entries from match cache")

    def _clean_epg_cache(self):
        """Clean EPG cache"""
        if len(self.epg_cache) > 10000:
            excess = len(self.epg_cache) - 8000
            # Remove random entries (simpler than tracking usage)
            keys_to_remove = list(self.epg_cache.keys())[:excess]
            for key in keys_to_remove:
                del self.epg_cache[key]
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"🧹 Cleaned {excess} entries from EPG cache")

    def _perform_deep_cleanup(self):
        """Perform comprehensive cleanup"""
        # Clear all search caches
        self._name_search_cache.clear()
        self._rytec_name_cache.clear()
        self._manual_cache.clear()

        # Reset statistics if they become too large
        if hasattr(self, '_match_cache_hits') and hasattr(self, '_match_cache_misses'):
            if self._match_cache_hits > 1000000 or self._match_cache_misses > 1000000:
                self._match_cache_hits = 0
                self._match_cache_misses = 0
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug("🔄 Reset cache statistics counters")

        # Force garbage collection
        import gc
        gc.collect()

        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug("🔄 Deep cleanup completed")

    def clean_channel_name(self, name, preserve_variants=False):
        """Clean channel name - preserve +1, +2, +HD patterns."""
        if not name:
            return ""

        cache_key = f"{name}_{preserve_variants}"
        if cache_key in self.mapping._clean_name_cache:
            return self.mapping._clean_name_cache[cache_key]

        try:
            cleaned = name.strip()

            # STEP 1: Preserva i pattern +1, +2, +HD, +4K, etc.
            plus_matches = self._plus_pattern.findall(cleaned)
            # STEP 2: Use precompiled pattern to remove special characters, ma preserva +
            cleaned = sub(r'[^\w\s\+\-àèéìíòóùúÀÈÉÌÍÒÓÙÚ]', ' ', cleaned)
            # STEP 3: Remove quality indicators BUT preserve those that start with +
            cleaned = self._quality_pattern.sub('', cleaned)
            # STEP 4: Convert to lowercase
            cleaned = cleaned.lower()
            # STEP 5: Remove parentheses and numbers in one go
            cleaned = sub(r'\s*\(\d+\)\s*', '', cleaned)  # Remove (7), (6), etc.
            cleaned = sub(r'\s*\(backup\)\s*', '', cleaned, flags=IGNORECASE)
            cleaned = sub(r'\s*\(.*?\)\s*', '', cleaned)  # Remove any parentheses content
            # STEP 6: Remove dots
            cleaned = cleaned.replace('.', ' ')
            # STEP 7: RESTORE +1, +2, +HD patterns after cleaning
            if plus_matches:
                # Remove duplicates and preserve order
                unique_plus = []
                for pattern in plus_matches:
                    pattern_lower = pattern.lower()
                    if pattern_lower not in unique_plus:
                        unique_plus.append(pattern_lower)

                # Add preserved patterns
                for pattern in unique_plus:
                    # Check if the pattern is already present (it may have survived cleaning)
                    if pattern not in cleaned:
                        cleaned += ' ' + pattern

            # STEP 8: REMOVE SPACES - IMPORTANT for matching
            cleaned = cleaned.replace(' ', '')
            # STEP 9: Minimal normalization
            cleaned = sub(r'[\\/_,;:]', '', cleaned).strip()

            self.mapping._clean_name_cache[cache_key] = cleaned
            return cleaned

        except Exception as e:
            logger.error(f"Error cleaning channel name '{name}': {str(e)}")
            # Fallback preserve i +
            fallback = name.lower().replace(' ', '').replace('(', '').replace(')', '') if name else ""
            return fallback

    def _search_case_insensitive_matches(self, channel_name, clean_name, tvg_id):
        """Search for matches with case-insensitive and number variations"""
        matches = []

        # Create different name variants including original case
        variants = [
            clean_name,                           # Original cleaned name
            clean_name.lower(),                   # lowercase
            clean_name.upper(),                   # UPPERCASE
            clean_name.title(),                   # Title Case
            clean_name.replace(' ', ''),          # Without spaces
            clean_name.replace(' ', '').lower(),  # Without spaces + lowercase
            clean_name.replace(' ', '').upper(),  # Without spaces + UPPERCASE
            clean_name.replace(' ', '').title(),  # Without spaces + Title Case
        ]

        # Add variants with shifted numbers
        words = clean_name.split()
        if len(words) == 2:
            word1, word2 = words
            # If one word is numeric and the other is textual
            if (word1.isdigit() and not word2.isdigit()) or (word2.isdigit() and not word1.isdigit()):
                if word1.isdigit():
                    number, text = word1, word2
                else:
                    number, text = word2, word1

                # Generate all possible combinations with different cases
                number_variants = [
                    f"{number}{text}",           # "20Mediaset"
                    f"{text}{number}",           # "Mediaset20"
                    f"{number} {text}",          # "20 Mediaset"
                    f"{text} {number}",          # "Mediaset 20"
                    f"{number}.{text}",          # "20.Mediaset"
                    f"{text}.{number}",          # "Mediaset.20"
                    f"{number}{text}.it",        # "20Mediaset.it"
                    f"{text}{number}.it",        # "Mediaset20.it"
                    f"{number}.{text}.it",       # "20.Mediaset.it"
                    f"{text}.{number}.it",       # "Mediaset.20.it"
                    # Add uppercase variants
                    f"{number}{text.upper()}",           # "20MEDIASET"
                    f"{text.upper()}{number}",           # "MEDIASET20"
                    f"{number} {text.upper()}",          # "20 MEDIASET"
                    f"{text.upper()} {number}",          # "MEDIASET 20"
                ]
                variants.extend(number_variants)

        # Remove duplicates
        variants = list(set(variants))

        # Search each variant in the Rytec database
        for variant in variants:
            # Search in Rytec basic
            for rytec_id, service_ref in self.mapping.rytec['basic'].items():
                if not service_ref:
                    continue

                # Case-insensitive exact match
                if rytec_id.lower() == variant.lower():
                    matches.append({
                        'type': 'rytec',
                        'sref': service_ref,
                        'name': f"Rytec: {rytec_id}",
                        'similarity': 1.0,
                        'priority': 95
                    })
                # Case-insensitive partial match
                elif variant.lower() in rytec_id.lower():
                    similarity = self._calculate_similarity(variant.lower(), rytec_id.lower())
                    if similarity > 0.7:
                        matches.append({
                            'type': 'rytec',
                            'sref': service_ref,
                            'name': f"Rytec: {rytec_id}",
                            'similarity': similarity,
                            'priority': 85
                        })

        return matches

    def normalize_service_reference(self, service_ref=None, for_epg=False):
        """Normalize service reference with correct satellite parameters."""
        if not service_ref or not isinstance(service_ref, str):
            return service_ref

        # If it's an IPTV reference, convert it correctly
        if service_ref.startswith('4097:'):
            parts = service_ref.split(':')
            if len(parts) < 11:
                parts += ['0'] * (11 - len(parts))

            # Extract DVB parameters from IPTV reference
            service_type = parts[2]
            service_id = parts[3]
            ts_id = parts[4]
            on_id = parts[5]
            namespace = parts[6]

            # Convert to correct DVB reference
            return f"1:0:{service_type}:{service_id}:{ts_id}:{on_id}:{namespace}:0:0:0:"

        parts = service_ref.split(':')
        if len(parts) < 11:
            parts += ['0'] * (11 - len(parts))

        # Correct satellite parameters
        if len(parts) > 6:
            namespace = parts[6]
            on_id = parts[5]

            # For satellite, ensure ONID and namespace are correct
            if namespace == '820000' and len(on_id) == 1:
                # Probably a terrestrial reference erroneously marked as satellite
                parts[5] = '13E'  # Hotbird position
            elif namespace == '820000' and on_id == '1':
                parts[5] = '13E'  # Correct ONID for Hotbird

        return ':'.join(parts)

    def optimize_matching(self):
        """Optimize channel map structures for faster matching."""
        self.mapping.optimized.clear()

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Debug DVB mapping keys: {list(self.mapping.dvb.keys())[:10] if self.mapping.dvb else 'EMPTY'}")
            logger.info(f"Total DVB channels: {len(self.mapping.dvb)}")

        for name, services in self.mapping.dvb.items():
            if not services:
                continue

            bouquet_services = [s for s in services if s["source"] == "bouquet"]

            satellite_services = [s for s in services if s["type"] == "satellite" and s["source"] == "lamedb"]

            other_services = [s for s in services if s not in bouquet_services + satellite_services]

            if bouquet_services:
                main_service = bouquet_services[0]
            elif satellite_services:
                main_service = satellite_services[0]
            elif other_services:
                main_service = other_services[0]
            else:
                continue

            clean_name = self.clean_channel_name(name)
            self.mapping.optimized[name] = main_service
            if clean_name not in self.mapping.optimized:
                self.mapping.optimized[clean_name] = main_service

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Optimized channel map built: {len(self.mapping.optimized)} entries")

    def classify_service_type(self, service_ref=None):
        """Classify service type based on service reference."""
        if not service_ref:
            return "unknown"

        parts = service_ref.split(':')
        if len(parts) < 11:
            return "unknown"

        service_type = parts[2]
        namespace = parts[6] if len(parts) > 6 else ""

        # Classify by namespace FIRST
        if namespace == "820000":
            return "satellite"
        elif namespace == "EEEE":
            return "dvb-t"
        elif namespace == "FFFF":
            return "dvb-c"
        elif service_type == "16":
            return "dvb-t"
        elif service_type == "10":
            return "dvb-c"
        elif service_type == "1":
            return "satellite"
        elif service_ref.startswith("4097:"):
            return "iptv"

        return "unknown"

    def filter_compatible_services(self, services):
        """Filter services"""
        compatible_services = []

        for service in services:
            service_ref = service['sref']
            service_type = service.get('type', 'unknown')

            # Handle DVB-T and DVB-C
            if service_type in ['terrestrial', 'cable']:
                # Skip DVB-T if configured to ignore them
                if config.plugins.m3uconverter.ignore_dvbt.value and self._is_dvb_t_service(service_ref):
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"🔧 Skipping DVB-T service: {service.get('name', 'Unknown')}")
                    continue
                # Keep DVB-C and non-ignored DVB-T
                compatible_services.append(service)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"🔧 Keeping terrestrial/cable service: {service.get('name', 'Unknown')}")
                continue

            # KEEP IPTV
            if service_type == 'iptv' or service_ref.startswith('4097:'):
                compatible_services.append(service)
                continue

            # FILTER ONLY SATELLITE (if incompatible)
            if service_type == 'satellite':
                if self._is_satellite_compatible(service.get('comment', '')):
                    compatible_services.append(service)
            else:
                # KEEP EVERYTHING ELSE
                compatible_services.append(service)

        return compatible_services

    def _is_dvb_t_service(self, sref):
        """Check if service is DVB-T by namespace EEEE"""
        if not sref:
            return False
        parts = sref.split(':')
        return len(parts) > 6 and parts[6] == 'EEEE'

    def _is_satellite_compatible(self, comment):
        """Check if satellite service is compatible with current configuration."""
        if not comment:
            return True

        comment_lower = comment.lower()

        # Complete list of satellites with all names
        main_satellites = [
            '13.0e', '13e', '13°e', 'hotbird',
            '19.2e', '19e', '19.2', 'astra19',
            '28.2e', '28e', '28.2', 'astra28',
            '23.5e', '23e', 'astra23',
            '5.0w', '5w', 'amazonas',
            '0.8w', 'thor',
            '4.8e', '4.8', 'sirius',
            '7.0w', '7w', 'nilesat',
            '9.0e', '9e', 'eutelsat9',
            '8.0w', 'express',
            '45.0e', 'intelsat',
            '42.0e', 'turksat',
            '39.0e', 'hellassat',
            '36e', 'eutelsat36',
            '33.0e', 'eutelsat33',
            '31.5e', '31.5',
            '30.0w', 'hispasat',
            '28.4e', '28.4',
            '26.0e', 'badr',
            '16.0e', 'eutelsat16',
            '15w', '15.0w', 'telstar',
            '1.9e', '1.9',
            '4.0w', 'amos'
        ]

        for satellite in main_satellites:
            if satellite in comment_lower:
                return True

        return False

    def _is_service_compatible(self, service_ref=None):
        """Check if service is compatible with current configuration"""
        if not service_ref:
            return True

        parts = service_ref.split(':')
        if len(parts) < 6:
            return None

        # If it's IPTV, always compatible
        if service_ref.startswith('4097:'):
            return True

        # Reads the satellites configured in the tuner automatically
        return self._is_satellite_compatible(service_ref)

    def _add_to_cache(self, cache_key, result, match_type):
        """Add match to cache"""
        if not result:
            return

        if match_type and match_type.count('_') > 2:
            parts = match_type.split('_')
            if 'manual' in parts:
                if 'rytec' in parts:
                    match_type = 'manual_rytec'
                elif 'dvb' in parts:
                    match_type = 'manual_dvb'
                elif 'dvbt' in parts:
                    match_type = 'manual_dvbt'
                else:
                    match_type = 'manual_db'
            elif 'rytec' in parts:
                match_type = 'rytec_auto'
            elif 'dvb' in parts:
                match_type = 'dvb_auto'
            else:
                match_type = 'auto'

        if len(self._match_cache) >= self._cache_max_size:
            items_to_remove = int(self._cache_max_size * 0.2)
            for key in list(self._match_cache.keys())[:items_to_remove]:
                del self._match_cache[key]

        self._match_cache[cache_key] = {
            'sref': result,
            'match_type': match_type,
            'timestamp': strftime("%Y-%m-%d %H:%M:%S"),
            'compatible': self._is_service_compatible(result)
        }

    def _parse_lamedb(self, lamedb_path="/etc/enigma2/lamedb"):
        """Parse both lamedb and lamedb5 using unified mapping."""
        paths_to_try = [
            "/etc/enigma2/lamedb5",
            "/etc/enigma2/lamedb"
        ]

        for lamedb_path in paths_to_try:
            if not fileExists(lamedb_path):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"Lamedb file not found: {lamedb_path}")
                continue

            try:
                with open(lamedb_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()

                # Check if it's a transponder file
                if content.strip().startswith("p:") and "services" not in content:
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.warning(f"Lamedb file {lamedb_path} appears to be a transponder list")
                    continue

                # Identify the file format
                if content.startswith("eDVB services /5/"):
                    self._parse_lamedb5_format(content)
                else:
                    self._parse_legacy_lamedb_format(content)

                # Filter incompatible services
                for name in list(self.mapping.dvb.keys()):
                    compatible_services = self.filter_compatible_services(self.mapping.dvb[name])
                    if compatible_services:
                        self.mapping.dvb[name] = compatible_services
                    else:
                        del self.mapping.dvb[name]

                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Parsed {0} unique compatible DVB channel names from {1}".format(len(self.mapping.dvb), lamedb_path))
                    # Debug found namespaces
                    namespaces = defaultdict(int)
                    for services in self.mapping.dvb.values():
                        for service in services:
                            if service["source"] == "lamedb5":
                                namespaces[service.get("namespace", "unknown")] += 1

                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info("Found namespaces: {0}".format(dict(namespaces)))
                return True

            except Exception as e:
                logger.error("Error parsing {0}: {1}".format(lamedb_path, str(e)))

        if config.plugins.m3uconverter.enable_debug.value:
            logger.error("Could not find or parse any lamedb file")
        return False

    def _parse_lamedb5_format(self, content):
        """Parse lamedb5 file format."""
        lines = content.split("\n")
        dvbt_count = 0
        total_count = 0

        for line in lines:
            if line.startswith("s:"):
                total_count += 1
                parts = line.split(",", 2)
                if len(parts) >= 2:
                    sref_parts = parts[0][2:].split(":")
                    if len(sref_parts) >= 6:
                        service_id = sref_parts[0]
                        namespace = sref_parts[1]
                        ts_id = sref_parts[2]
                        on_id = sref_parts[3]
                        service_type = sref_parts[4]

                        # Handle namespace correctly
                        if namespace == "00820000":
                            namespace = "820000"
                            service_type = "1"
                        elif namespace.startswith("eeee"):
                            namespace = "EEEE"
                            service_type = "16"
                            dvbt_count += 1  # COUNT DVB-T
                        elif namespace.startswith("ffff"):
                            namespace = "FFFF"
                            service_type = "10"
                        else:
                            if len(namespace) > 4:
                                namespace = namespace[:4]

                        channel_name = parts[1].strip('"')
                        service_ref = f"1:0:{service_type}:{service_id}:{ts_id}:{on_id}:{namespace}:0:0:0:"

                        clean_name = self.clean_channel_name(channel_name)

                        self.mapping.dvb[clean_name].append({
                            "sref": service_ref,
                            "type": self.classify_service_type(service_ref),
                            "source": "lamedb5",
                            "service_id": service_id,
                            "ts_id": ts_id,
                            "on_id": on_id,
                            "namespace": namespace
                        })
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"🔍 PARSED: {total_count} services, {dvbt_count} DVB-T services")

    def _parse_legacy_lamedb_format(self, content):
        """Parse traditional lamedb file format."""
        lines = content.split("\n")
        for line in lines:
            if line.startswith("s:"):
                parts = line.split(",")
                if len(parts) >= 2:
                    sref_part = parts[0]
                    channel_name = parts[1].strip('"')
                    sref_parts = sref_part.split(":")
                    if len(sref_parts) >= 6:
                        service_id = sref_parts[1]
                        on_id = sref_parts[2]
                        if len(on_id) > 4:
                            on_id = on_id[:4]
                        ts_id = sref_parts[3]
                        service_type = sref_parts[4]

                        service_ref = "1:0:{0}:{1}:{2}:{3}:820000:0:0:0:".format(
                            service_type, service_id, ts_id, on_id
                        )
                        clean_name = self.clean_channel_name(channel_name)
                        self.mapping.dvb[clean_name].append({
                            "sref": service_ref,
                            "type": self.classify_service_type(service_ref),
                            "source": "lamedb",
                            "service_id": service_id,
                            "ts_id": ts_id,
                            "on_id": on_id
                        })

    def _parse_rytec_channels(self, rytec_path=None):
        """Parse rytec.channels.xml using unified mapping."""
        rytec_paths = [
            "/etc/epgimport/rytec.channels.xml",
            "/usr/lib/enigma2/python/Plugins/Extensions/EPGImport/rytec.channels.xml"
        ]

        if rytec_path and fileExists(rytec_path):
            final_path = rytec_path
        else:
            final_path = None
            for path in rytec_paths:
                if fileExists(path):
                    final_path = path
                    break

        if not final_path:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.warning("Rytec file not found in any path: %s", rytec_paths)
            return

        if not fileExists(final_path):
            if config.plugins.m3uconverter.enable_debug.value:
                logger.warning("Rytec file not found: %s", final_path)
            return

        try:
            with open(final_path, "r", encoding="utf-8") as f:
                content = f.read()

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Rytec file found, size: %d bytes", len(content))

            pattern = r'(<!--\s*([^>]+)\s*-->)?\s*<channel id="([^"]+)">([^<]+)</channel>\s*(?:<!--\s*([^>]+)\s*-->)?'
            matches = findall(pattern, content)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Found %d channel entries in rytec file", len(matches))

            with self._rytec_lock:
                for match in matches:
                    comment_before, source_info, channel_id, service_ref, comment_after = match
                    comment = comment_before or comment_after or ""

                    # Extract the real channel name
                    channel_name = self._extract_real_channel_name(comment)

                    normalized_ref = self.normalize_service_reference(service_ref, for_epg=True)

                    if self._is_service_compatible(normalized_ref):
                        # Extended database with all info
                        self.mapping.rytec['extended'][channel_id].append({
                            'sref': normalized_ref,
                            'comment': comment.strip(),
                            'channel_name': channel_name,
                            'source_type': self._get_source_type(comment),
                            'sat_position': self._extract_satellite_position(comment)
                        })

                        # KEEP COMPATIBILITY
                        if channel_id not in self.mapping.rytec['basic']:
                            self.mapping.rytec['basic'][channel_id] = normalized_ref

                        clean_base_id = self.clean_channel_name(channel_id.split('.')[0])
                        self.mapping.rytec['clean'][clean_base_id] = normalized_ref

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Parsed %d Rytec channels with extended info", len(self.mapping.rytec['extended']))

        except Exception as e:
            logger.error("Error parsing rytec.channels.xml: %s", str(e))

    def _parse_with_lxml(self, epg_path):
        """Parse with lxml library."""
        try:
            parser = etree.XMLParser(encoding='utf-8', recover=True)
            tree = etree.parse(epg_path, parser)
            root = tree.getroot()

            if root is None:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("Failed to get root element")
                return False

            # Find all channel elements
            channels = root.findall('.//channel')
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Found {len(channels)} channel elements with lxml")

            added_count = 0
            for channel in channels:
                try:
                    channel_id = channel.get('id')
                    if not channel_id:
                        continue

                    # Find display name
                    display_name_elem = channel.find('display-name')
                    if display_name_elem is None or not display_name_elem.text:
                        continue

                    display_name = display_name_elem.text.strip()

                    # Add to mapping
                    clean_name = self.clean_channel_name(display_name, preserve_variants=True)
                    self.mapping.rytec['extended'][channel_id] = [{
                        'channel_name': display_name,
                        'sref': None,
                        'source_type': 'epgshare',
                        'original_id': channel_id,
                        'clean_name': clean_name
                    }]

                    self.mapping.rytec['basic'][channel_id] = None
                    added_count += 1

                except Exception as e:
                    logger.warning(f"Error parsing channel: {str(e)}")
                    continue
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Added {added_count} channels with lxml parsing")
            return added_count > 0

        except Exception as e:
            logger.error(f"lxml parsing failed: {str(e)}")
            return False

    def _parse_with_elementtree(self, epg_path):
        """Parse with ElementTree fallback."""
        try:
            tree = ET.parse(epg_path)
            root = tree.getroot()

            channels = root.findall('.//channel')
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Found {len(channels)} channel elements with ElementTree")

            added_count = 0
            for channel in channels:
                try:
                    channel_id = channel.get('id')
                    if not channel_id:
                        continue

                    display_name_elem = channel.find('display-name')
                    if display_name_elem is None or not display_name_elem.text:
                        continue

                    display_name = display_name_elem.text.strip()
                    clean_name = self.clean_channel_name(display_name, preserve_variants=True)

                    service_ref = self._generate_dvb_service_ref(display_name, channel_id)

                    self.mapping.rytec['extended'][channel_id].append({
                        'channel_name': display_name,
                        'sref': service_ref,
                        'source_type': 'epgshare',
                        'original_id': channel_id,
                        'clean_name': clean_name
                    })

                    self.mapping.rytec['basic'][channel_id] = service_ref
                    added_count += 1

                except Exception as e:
                    logger.warning(f"Error parsing channel: {str(e)}")
                    continue

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Added {added_count} channels with ElementTree parsing")
            return added_count > 0

        except Exception as e:
            logger.error(f"ElementTree parsing failed: {str(e)}")
            return False

    def _parse_existing_bouquets(self, bouquet_dir="/etc/enigma2"):
        """Parse all existing bouquets for current service references."""
        bouquet_files = []

        # First read the main bouquets.tv file
        bouquets_file = join(bouquet_dir, "bouquets.tv")
        if fileExists(bouquets_file):
            with open(bouquets_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if 'FROM BOUQUET "' in line:
                        match = search(r'FROM BOUQUET "([^"]+)"', line)
                        if match:
                            bouquet_files.append(join(bouquet_dir, match.group(1)))

        # Add all userbouquet files
        for filename in listdir(bouquet_dir):
            if filename.startswith('userbouquet.') and filename.endswith('.tv'):
                bouquet_files.append(join(bouquet_dir, filename))

        # Parse each bouquet
        for bouquet_file in bouquet_files:
            if not fileExists(bouquet_file):
                continue

            try:
                with open(bouquet_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                    service_pattern = r'#SERVICE (\d+:\d+:\d+:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:[^:]+:)'
                    matches = findall(service_pattern, content)

                    for service_ref in matches:
                        if not service_ref.startswith('4097:'):  # Ignore IPTV services
                            desc_pattern = r'#DESCRIPTION (.+)\n'
                            desc_match = search(desc_pattern, content[content.find(service_ref):])
                            channel_name = desc_match.group(1).strip() if desc_match else "Unknown"

                            clean_name = self.clean_channel_name(channel_name)

                            self.mapping.dvb[clean_name].append({
                                "sref": service_ref,
                                "type": self.classify_service_type(service_ref),
                                "source": "bouquet",
                                "service_id": service_ref.split(':')[3],
                                "ts_id": service_ref.split(':')[4],
                                "on_id": service_ref.split(':')[5]
                            })

            except Exception as e:
                logger.error(f"Error parsing bouquet {bouquet_file}: {str(e)}")

    def _parse_epgshare_for_mapping(self, epg_path):
        """Robust EPGShare parsing with lxml."""
        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Parsing EPGShare file: {epg_path}")

            # Clear existing EPGShare entries
            self._clear_epgshare_entries()

            # Check file exists and has content
            if not fileExists(epg_path):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("EPGShare file does not exist")
                return False

            file_size = getsize(epg_path)
            if file_size == 0:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("EPGShare file is empty")
                return False
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"File size: {file_size} bytes")

            # Parse with lxml
            if LXML_AVAILABLE:
                return self._parse_with_lxml(epg_path)
            else:
                return self._parse_with_elementtree(epg_path)

        except Exception as e:
            logger.error(f"EPGShare parsing error: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def reset_caches(self, clear_match_cache=False, reset_stats=True):
        """Reset statistics but preserve the cache between conversions"""
        # RESET statistics by default at the start of each conversion
        if reset_stats:
            if clear_match_cache:
                self._match_cache_hits = 0
                self._match_cache_misses = 0

            self._stats_counters = {
                'rytec_matches': 0,
                'dvb_matches': 0,
                'dvbt_matches': 0,
                'fallback_matches': 0,
                'manual_db_matches': 0
            }
            # EPG Cache - this one can be cleared
            self.epg_cache.clear()
            self.epg_cache_hits = 0
            self.epg_cache_misses = 0
            # Other statistics
            self._incompatible_matches = 0
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("🔄 Statistics counters RESET for new conversion")
        else:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug("🔄 Caches optimized - statistics preserved")

        # Clear match cache only if explicitly requested
        if clear_match_cache:
            match_cache_size = len(self._match_cache)
            self._match_cache.clear()
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Match cache cleared: {match_cache_size} entries removed")

    def _clear_epgshare_entries(self):
        """Clear all EPGShare entries."""
        keys_to_remove = []
        for channel_id, variants in self.mapping.rytec['extended'].items():
            for variant in variants:
                if variant.get('source_type') == 'epgshare':
                    keys_to_remove.append(channel_id)
                    break

        for channel_id in keys_to_remove:
            del self.mapping.rytec['extended'][channel_id]
            if channel_id in self.mapping.rytec['basic']:
                del self.mapping.rytec['basic'][channel_id]
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Cleared {len(keys_to_remove)} EPGShare entries")

    def _clear_dvbt_services(self):
        """Remove DVB-T services based on database mode."""
        # KEEP DVB-T for full and dtt modes
        if self.database_mode in ["full", "dtt"]:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("🔧 Keeping DVB-T services (mode: full/dtt)")
            return 0

        removed_count = 0
        for channel_name in list(self.mapping.dvb.keys()):
            filtered_services = [
                service for service in self.mapping.dvb[channel_name]
                if not service.get('sref', '').split(':')[6] == 'EEEE'
            ]
            removed_count += (len(self.mapping.dvb[channel_name]) - len(filtered_services))
            if filtered_services:
                self.mapping.dvb[channel_name] = filtered_services
            else:
                del self.mapping.dvb[channel_name]
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"🔧 Removed {removed_count} DVB-T (EEEE) services (mode: {self.database_mode})")
        return removed_count

    def _enhanced_search_short_names(self, clean_name, original_name):
        """Enhanced search for short names and numbered channels with case-insensitive matching."""
        matches = []

        # Use the dedicated function for case-insensitive matching
        case_insensitive_matches = self._search_case_insensitive_matches(original_name, clean_name, "")
        matches.extend(case_insensitive_matches)

        # Create different name variants including all case combinations
        variants = [
            clean_name,                           # Original cleaned name
            clean_name.lower(),                   # lowercase
            clean_name.upper(),                   # UPPERCASE
            clean_name.title(),                   # Title Case
            clean_name.replace(' ', ''),          # Without spaces
            clean_name.replace(' ', '').lower(),  # Without spaces + lowercase
            clean_name.replace(' ', '').upper(),  # Without spaces + UPPERCASE
            clean_name.replace(' ', '').title(),  # Without spaces + Title Case
        ]

        # Add variants with shifted numbers
        words = clean_name.split()
        if len(words) == 2:
            word1, word2 = words
            # If one word is numeric and the other textual
            if (word1.isdigit() and not word2.isdigit()) or (word2.isdigit() and not word1.isdigit()):
                if word1.isdigit():
                    number, text = word1, word2
                else:
                    number, text = word2, word1

                # Generate all possible combinations with different cases
                number_variants = [
                    f"{number}{text}",           # "20Mediaset"
                    f"{text}{number}",           # "Mediaset20"
                    f"{number} {text}",          # "20 Mediaset"
                    f"{text} {number}",          # "Mediaset 20"
                    f"{number}.{text}",          # "20.Mediaset"
                    f"{text}.{number}",          # "Mediaset.20"
                    f"{number}{text}.it",        # "20Mediaset.it"
                    f"{text}{number}.it",        # "Mediaset20.it"
                    f"{number}.{text}.it",       # "20.Mediaset.it"
                    f"{text}.{number}.it",       # "Mediaset.20.it"
                    # Uppercase variants
                    f"{number}{text.upper()}",           # "20MEDIASET"
                    f"{text.upper()}{number}",           # "MEDIASET20"
                    f"{number} {text.upper()}",          # "20 MEDIASET"
                    f"{text.upper()} {number}",          # "MEDIASET 20"
                ]
                variants.extend(number_variants)

        # Remove duplicates
        variants = list(set(variants))

        # Search each variant in the Rytec database
        for variant in variants:
            # Search in Rytec basic
            for rytec_id, service_ref in self.mapping.rytec['basic'].items():
                if not service_ref:
                    continue

                # Case-insensitive exact match
                if rytec_id.lower() == variant.lower():
                    matches.append({
                        'type': 'rytec',
                        'sref': service_ref,
                        'name': f"Rytec: {rytec_id}",
                        'similarity': 1.0,
                        'priority': 95
                    })
                # Case-insensitive partial match
                elif variant.lower() in rytec_id.lower():
                    similarity = self._calculate_similarity(variant.lower(), rytec_id.lower())
                    if similarity > 0.7:
                        matches.append({
                            'type': 'rytec',
                            'sref': service_ref,
                            'name': f"Rytec: {rytec_id}",
                            'similarity': similarity,
                            'priority': 85
                        })

        # Search in the DVB database with case-insensitive matching
        for db_name, services in self.mapping.dvb.items():
            if not services:
                continue

            # Case-insensitive exact match
            if clean_name.lower() == db_name.lower():
                for service in services:
                    service_type = 'dvbt' if self._is_dvb_t_service(service['sref']) else 'dvb'
                    matches.append({
                        'type': service_type,
                        'sref': service['sref'],
                        'name': f"{service_type.upper()}: {db_name}",
                        'similarity': 1.0,
                        'priority': 90
                    })
            # Case-insensitive partial match
            elif clean_name.lower() in db_name.lower():
                similarity = self._calculate_similarity(clean_name.lower(), db_name.lower())
                if similarity > 0.7:
                    for service in services:
                        service_type = 'dvbt' if self._is_dvb_t_service(service['sref']) else 'dvb'
                        matches.append({
                            'type': service_type,
                            'sref': service['sref'],
                            'name': f"{service_type.upper()}: {db_name}",
                            'similarity': similarity,
                            'priority': 80
                        })

        return matches

    def _enhanced_rytec_name_search(self, clean_name, original_name):
        """Enhanced search in Rytec database by name."""
        matches = []

        if not clean_name or not self.mapping.rytec['basic']:
            return matches

        clean_lower = clean_name.lower()

        for rytec_id, service_ref in self.mapping.rytec['basic'].items():
            if not service_ref:
                continue

            rytec_lower = rytec_id.lower()

            # Exact match in Rytec ID
            if clean_lower == rytec_lower:
                matches.append({
                    'sref': service_ref,
                    'name': f"Rytec: {rytec_id}",
                    'similarity': 1.0
                })
                continue

            # Partial match with similarity
            similarity = self._calculate_similarity(clean_lower, rytec_lower)
            if similarity > self.similarity_threshold_rytec:
                matches.append({
                    'sref': service_ref,
                    'name': f"Rytec: {rytec_id}",
                    'similarity': similarity
                })

        return matches

    def match_with_manual_database(self, channel_name, clean_name):
        """Wrapper for manual database matching"""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"🎯 MANUAL DB CALLED: '{channel_name}' -> '{clean_name}'")

        if hasattr(self, 'manual_db') and self.manual_db:
            result = self.manual_db.find_mapping(channel_name, clean_name=clean_name)

            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"🎯 MANUAL DB RESULT: {result is not None}")

        if result:
            service_ref = result.get('assigned_sref')
            # if config.plugins.m3uconverter.enable_debug.value:
            #     logger.debug(f"✅ MANUAL DB MATCH: '{channel_name}' -> {service_ref}")

            # FORCE match_type to 'manual_db' for all manual database matches
            match_type = 'manual_db'

            return service_ref, match_type

        return None, None

    def _find_best_service_match(self, clean_name, tvg_id=None, original_name="", channel_url=None):
        """Universal matching with IMPROVED handling"""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"🔍 MATCH: '{original_name}' -> tvg_id: '{tvg_id}' -> clean: '{clean_name}'")

        # Initialize statistics if not already set
        if not hasattr(self, '_stats_counters'):
            self._stats_counters = {
                'rytec_matches': 0,
                'dvb_matches': 0,
                'dvbt_matches': 0,
                'fallback_matches': 0,
                'manual_db_matches': 0
            }

        cache_key = f"{clean_name}_{tvg_id}"

        # 1. FIRST: Manual Database (highest priority) - SOLO QUESTA!
        service_ref, match_type = self.match_with_manual_database(original_name, clean_name)
        if service_ref:
            # Save to cache
            self._add_to_cache(cache_key, service_ref, match_type)
            return service_ref, match_type

        # 2. SECOND: Check cache
        if cache_key in self._match_cache:
            self._match_cache_hits += 1
            cached = self._match_cache[cache_key]
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"✅ CACHE HIT: {cache_key} -> {cached['match_type']}")
            return cached['sref'], cached['match_type']

        # 3. ENHANCED: Special handling for short names and numbered channels
        if len(clean_name) <= 5 or any(char.isdigit() for char in clean_name):
            # Try enhanced search first for short names
            enhanced_matches = self._enhanced_search_short_names(clean_name, original_name)

            # Also search with case-insensitive matching
            case_insensitive_matches = self._search_case_insensitive_matches(clean_name, clean_name, tvg_id)
            enhanced_matches.extend(case_insensitive_matches)

            if enhanced_matches:
                # Use the best enhanced match
                best_enhanced = max(enhanced_matches, key=lambda x: (x.get('priority', 0), x['similarity']))
                service_ref = best_enhanced['sref']
                match_type = f"{best_enhanced['type']}_enhanced"

                # Count the match
                if 'rytec' in match_type:
                    self._stats_counters['rytec_matches'] += 1
                elif 'dvb' in match_type:
                    if self._is_dvb_t_service(service_ref):
                        self._stats_counters['dvbt_matches'] += 1
                    else:
                        self._stats_counters['dvb_matches'] += 1

                # Save to cache
                self._add_to_cache(cache_key, service_ref, match_type)
                return service_ref, match_type

        # 4. ENHANCED RYTEC SEARCH - Multiple format variants
        if self.database_mode in ["full", "both", "rytec"] and tvg_id and tvg_id.lower() != "none":
            # Try multiple Rytec ID formats
            rytec_variants = self._generate_rytec_variants(tvg_id, clean_name)

            for variant in rytec_variants:
                if variant in self.mapping.rytec['basic']:
                    service_ref = self.mapping.rytec['basic'][variant]
                    if service_ref and self._is_service_compatible(service_ref):
                        match_type = 'rytec_exact'
                        break  # Use first valid match

        # 4.1 ENHANCED RYTEC NAME SEARCH
        if (not service_ref and
                self.database_mode in ["full", "both", "rytec"] and
                clean_name and len(clean_name) >= 2):

            rytec_matches = self._enhanced_rytec_name_search(clean_name, original_name)
            if rytec_matches:
                best_match = max(rytec_matches, key=lambda x: x['similarity'])
                if best_match['similarity'] > self.similarity_threshold_rytec:
                    service_ref = best_match['sref']
                    match_type = 'rytec_name'
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug(f"✅ Rytec name match: {original_name} -> {best_match['name']}")

        # 5. RYTEC KEYWORD SEARCH - Use configurable similarity threshold
        if (not service_ref and
                self.database_mode in ["full", "both", "rytec"] and
                clean_name and len(clean_name) >= 2):

            keyword_matches = self._find_rytec_ids_by_keyword(clean_name)
            if keyword_matches:
                # Take the match with the highest similarity
                best_keyword_match = max(keyword_matches, key=lambda x: x['similarity'])

                # Use the specific Rytec threshold from config
                if best_keyword_match['similarity'] > self.similarity_threshold_rytec:
                    service_ref = best_keyword_match['sref']
                    match_type = 'rytec_keyword'
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug(f"✅ Rytec keyword match: {original_name} -> {best_keyword_match['name']}")

        # 6. DVB SEARCH - Use configurable similarity threshold
        if not service_ref and self.database_mode in ["full", "both", "dvb"]:
            if clean_name in self.mapping.optimized:
                dvb_service = self.mapping.optimized[clean_name]
                service_ref = dvb_service['sref']
                if self._is_dvb_t_service(service_ref):
                    match_type = 'dvb_t'
                else:
                    match_type = 'dvb_s'

        # 7. SPECIFIC DVB-T SEARCH
        if not service_ref and self.database_mode in ["full", "dtt"]:
            dvbt_match = self._find_dvbt_match(clean_name)
            if dvbt_match:
                service_ref = dvbt_match
                match_type = 'dvb_t'

        # 8. IPTV FALLBACK
        if not service_ref and channel_url:
            service_ref = self._generate_service_reference(channel_url)
            match_type = 'iptv_fallback'

        # FINAL: Count the match based on final result
        if service_ref:
            if 'manual_' in match_type:  # ✅ Questo conta TUTTI i manual matches
                self._stats_counters['manual_db_matches'] += 1
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"📊 COUNTED AS MANUAL DB: {match_type}")
            elif 'rytec' in match_type:
                self._stats_counters['rytec_matches'] += 1
            elif 'dvb' in match_type:
                if self._is_dvb_t_service(service_ref):
                    self._stats_counters['dvbt_matches'] += 1
                else:
                    self._stats_counters['dvb_matches'] += 1
            elif 'enhanced' in match_type:
                if 'rytec' in match_type:
                    self._stats_counters['rytec_matches'] += 1
                elif 'dvb' in match_type:
                    if self._is_dvb_t_service(service_ref):
                        self._stats_counters['dvbt_matches'] += 1
                    else:
                        self._stats_counters['dvb_matches'] += 1
            else:
                self._stats_counters['fallback_matches'] += 1
        else:
            self._stats_counters['fallback_matches'] += 1

        # Save to cache
        if service_ref:
            self._add_to_cache(cache_key, service_ref, match_type)

        if not service_ref:
            service_ref = None
            match_type = 'no_match'

        return service_ref, match_type

    def _find_dvbt_match(self, clean_name):
        """Fast DVB-T matching"""
        try:
            # Use existing optimized mapping first
            if clean_name in self.mapping.optimized:
                service = self.mapping.optimized[clean_name]
                if service.get('sref', '').split(':')[6] == 'EEEE':
                    return service['sref']

            # Limited direct search
            for service_name, services in list(self.mapping.dvb.items()):
                for service in services:
                    if service.get('sref', '').split(':')[6] == 'EEEE':
                        service_clean = self.clean_channel_name(service_name)
                        if service_clean == clean_name:
                            return service['sref']
            return None
        except Exception as e:
            logger.error(f"Error in DVB-T matching: {str(e)}")
            return None

    def _find_rytec_ids_by_keyword(self, keyword):
        """Search the Rytec database for channels containing the keyword in ID or name"""
        matches = []
        rytec_data = self.mapping.rytec['basic']

        if not rytec_data:
            return matches

        keyword_lower = keyword.lower()

        for rytec_id, service_ref in rytec_data.items():
            if not service_ref:
                continue

            rytec_id_lower = rytec_id.lower()

            # Extract the first part before any dot
            id_first_part = rytec_id_lower.split('.')[0]

            # Check if the keyword matches
            if (keyword_lower == id_first_part or
                    keyword_lower in id_first_part or
                    keyword_lower in rytec_id_lower):

                similarity = 0.7 if keyword_lower == id_first_part else 0.6
                matches.append({
                    'name': f"Rytec ID: {rytec_id}",
                    'sref': service_ref,
                    'similarity': similarity,
                    'type': 'rytec',
                    'priority': 60
                })

        return matches[:5]

    def _extract_real_channel_name(self, comment):
        """Extract the real channel name from the comment."""
        if not comment:
            return ""

        parts = comment.split('-->')
        if len(parts) > 1:
            return parts[-1].strip()

        return comment.strip()

    def _extract_satellite_position(self, comment):
        """Extract the satellite position from the comment."""
        position_match = search(r'(\d+\.\d+[EW])', comment)
        return position_match.group(1) if position_match else None

    def _extract_epg_url_from_m3u(self, m3u_path):
        """Search for an EPG URL in M3U file comments."""
        try:
            with open(m3u_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

            # Look for EPG URL in comments
            epg_match = search(r'#EXTEPGURL:?(.*)$', content, MULTILINE)
            if epg_match:
                return epg_match.group(1).strip()

            # Also check other common formats
            epg_match = search(r'#EXTVLCOPT:epg-url=(.*)$', content, MULTILINE)
            if epg_match:
                return epg_match.group(1).strip()

            return None
        except:
            return None

    def _convert_to_rytec_format(self, tvg_id):
        """Convert M3U channel IDs to Rytec format using LANGUAGE_TO_COUNTRY mapping."""
        if not tvg_id:
            return tvg_id

        # Get country code safely
        country_code = getattr(self, 'country_code', '')
        if callable(country_code):
            country_code = country_code()

        # Check for direct matches first
        if tvg_id in self.mapping.rytec['basic']:
            return tvg_id
        if tvg_id.lower() in self.mapping.rytec['basic']:
            return tvg_id.lower()

        # Generate suffixes dynamically from LANGUAGE_TO_COUNTRY
        country_suffixes = [
            f".{code}" for code in LANGUAGE_TO_COUNTRY.keys()
            if len(code) == 2 and code.isalpha()
        ]

        # Check if the ID ends with any valid country suffix
        for suffix in country_suffixes:
            if tvg_id.endswith(suffix):
                base_name = tvg_id[:-len(suffix)]

                # Generate all possible Rytec name variations
                variations = self._generate_rytec_variations(base_name, country_code)

                # Try to find a valid match in Rytec mappings
                for variation in variations:
                    if variation in self.mapping.rytec['basic']:
                        return variation
                break

        # Return the original if no match found
        return tvg_id

    def _generate_clean_rytec_id(self, channel_name, service_ref):
        """Generate Rytec ID from name"""
        if not channel_name:
            return "unknown"

        clean_name = channel_name.lower()
        clean_name = sub(r'[^a-z0-9]', '', clean_name)

        # Add country code if available
        country_code = ""
        if service_ref:
            # country_code = self._get_country_from_service_ref(service_ref)
            country_code = config.plugins.m3uconverter.language.value

        if country_code:
            return f"{clean_name}.{country_code}"
        else:
            return clean_name

    def _generate_epg_channels_file(self, epg_data, bouquet_name):
        """Generate channels.xml file with correct service references."""
        epgimport_path = "/etc/epgimport"
        epg_filename = f"{bouquet_name}.channels.xml"
        epg_path = join(epgimport_path, epg_filename)
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Generating EPG channels file with {len(epg_data)} entries")

        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"EPG_DATA DEBUG: {len(epg_data)} entries received")
            if epg_data:
                logger.info(f"First entry: {epg_data[0]}")

            channel_entries = []
            cache_stats = {'rytec': 0, 'dvb': 0, 'dvbt': 0, 'fallback': 0}
            processed_count = 0

            for channel in epg_data:
                channel_name = channel.get('name', 'Unknown')
                tvg_id = channel.get('tvg_id', '')
                service_ref = channel.get('sref', '')
                match_type = channel.get('match_type', 'iptv_fallback')

                # DEBUG: log every 10 processed channels
                if processed_count % 10 == 0:
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug(f"Processing channel {processed_count}: {channel_name} -> {match_type}")

                # Ensure service_ref is not empty
                if not service_ref:
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.warning(f"Skipping channel without service_ref: {channel_name}")
                    continue

                # Use the correct EPG ID method
                channel_id = self._get_correct_epg_id(channel_name, tvg_id, service_ref)

                # Count match_type correctly
                if 'rytec' in match_type:
                    cache_stats['rytec'] += 1
                elif 'dvb' in match_type:
                    if self._is_dvb_t_service(service_ref):
                        cache_stats['dvbt'] += 1
                    else:
                        cache_stats['dvb'] += 1
                else:
                    cache_stats['fallback'] += 1

                # Create the correct XML entry
                entry = f'  <!-- {channel_name} [{match_type}] --><channel id="{channel_id}">{service_ref}</channel>\n'
                channel_entries.append(entry)
                processed_count += 1

            # Write the file only if we have entries
            if not channel_entries:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("NO CHANNEL ENTRIES TO WRITE!")
                    logger.error(f"EPG data had {len(epg_data)} entries but 0 were processed")
                return False

            # SINGLE WRITE
            with open(epg_path, 'w', encoding="utf-8", buffering=65536) as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write('<channels>\n')
                f.writelines(channel_entries)
                f.write('</channels>\n')

            # VERIFY: Check that the file has been written
            file_size = getsize(epg_path) if exists(epg_path) else 0
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"EPG file written: {epg_path} ({file_size} bytes, {len(channel_entries)} entries)")
                logger.info(f"Optimized EPG channels file created: {len(channel_entries)} entries")
                logger.info(f"EPG Match stats - Rytec: {cache_stats.get('rytec', 0)}, DVB-S: {cache_stats.get('dvb', 0)}, DVB-T: {cache_stats.get('dvbt', 0)}, Fallback: {cache_stats.get('fallback', 0)}")
                logger.info("========= debug_epg_mapping =========")
            return True

        except Exception as e:
            logger.error(f"Error generating EPG channels file: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def _generate_epgshare_sources_file(self, bouquet_name, epg_url=None):
        """Generate sources.xml for EPGShare mode with incremental updates."""
        epgimport_path = "/etc/epgimport"
        sources_path = join(epgimport_path, "ArchimedeConverter.sources.xml")

        try:
            if not fileExists(epgimport_path):
                mkdir(epgimport_path)

            # If epg_url not provided, use language-based URL
            if not epg_url:
                language = config.plugins.m3uconverter.language.value
                epg_url = self._get_epg_url_for_language(language)

            # Read existing content or create new
            if fileExists(sources_path):
                try:
                    with open(sources_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                except:
                    content = '<?xml version="1.0" encoding="utf-8"?>\n<sources>\n</sources>'
            else:
                content = '<?xml version="1.0" encoding="utf-8"?>\n<sources>\n</sources>'

            # Check if this bouquet already exists in sources
            existing_pattern = rf'<source type="gen_xmltv"[^>]*channels="{escape(bouquet_name)}\.channels\.xml"[^>]*>'
            if search(existing_pattern, content):
                # Remove existing entry to update it
                content = sub(rf'<source type="gen_xmltv"[^>]*channels="{escape(bouquet_name)}\.channels\.xml"[^>]*>.*?</source>',
                              '', content, flags=DOTALL)

            # Create the new source entry
            new_source = f'    <source type="gen_xmltv" nocheck="1" channels="{bouquet_name}.channels.xml">\n'
            new_source += f'      <description>{bouquet_name}</description>\n'
            new_source += f'      <url><![CDATA[{epg_url}]]></url>\n'
            new_source += '    </source>\n'

            # Add to existing sourcecat or create new one
            sourcecat_pattern = r'<sourcecat sourcecatname="Archimede Converter">(.*?)</sourcecat>'
            sourcecat_match = search(sourcecat_pattern, content, DOTALL)

            if sourcecat_match:
                # Add to existing sourcecat
                existing_content = sourcecat_match.group(1)
                updated_content = existing_content + '\n' + new_source
                content = content.replace(sourcecat_match.group(0),
                                          f'<sourcecat sourcecatname="Archimede Converter">{updated_content}</sourcecat>')
            else:
                # Create new sourcecat
                new_sourcecat = '  <sourcecat sourcecatname="Archimede Converter">\n'
                new_sourcecat += new_source
                new_sourcecat += '  </sourcecat>\n'

                # Insert before closing </sources> tag
                if '</sources>' in content:
                    content = content.replace('</sources>', new_sourcecat + '</sources>')
                else:
                    content += new_sourcecat

            # Clean up excessive empty lines in the entire file
            content = sub(r'\n\s*\n', '\n\n', content)

            # Ensure proper indentation
            lines = content.split('\n')
            cleaned_lines = []
            for line in lines:
                # Remove lines with only whitespace
                if line.strip() == '':
                    continue
                cleaned_lines.append(line)

            content = '\n'.join(cleaned_lines)

            # Write the file
            with open(sources_path, 'w', encoding='utf-8') as f:
                f.write(content)

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"✅ EPG source UPDATED for: {bouquet_name}")
            return True

        except Exception as e:
            logger.error(f"❌ Error generating EPG sources: {str(e)}")
            return False

    def _generate_rytec_variations(self, base_name, country_code):
        """Generate all possible Rytec ID variations"""
        variations = [
            base_name,
            base_name.lower(),
            base_name.replace(' ', '.'),
            base_name.replace(' ', ''),
            base_name.replace('.', ''),
        ]

        # Add variations with country code
        if country_code and country_code != 'all':
            variations.extend([
                f"{base_name}.{country_code}",
                f"{base_name.lower()}.{country_code}",
            ])

            # Also use the mapping from the dictionary
            country_from_dict = LANGUAGE_TO_COUNTRY.get(country_code.lower())
            if country_from_dict and country_from_dict != 'ALL':
                variations.extend([
                    f"{base_name}.{country_from_dict.lower()}",
                    f"{base_name.lower()}.{country_from_dict.lower()}",
                ])

        return variations

    def _generate_rytec_style_id(self, channel_name, service_ref):
        """Generate Rytec-style ID that should work with EPG XMLTV"""
        if not channel_name:
            return "unknown"

        clean_name = self.clean_channel_name(channel_name)
        clean_name = clean_name.replace(' ', '.').lower()
        clean_name = sub(r'[^a-z0-9.]', '', clean_name)

        country_suffix = ""
        if service_ref:
            country_code = self._get_country_from_service_ref(service_ref)
            if country_code:
                country_suffix = f".{country_code}"

        if len(clean_name) > 30:
            clean_name = clean_name[:30]

        return f"{clean_name}{country_suffix}"

    def _generate_hybrid_sref(self, dvb_sref, url=None, for_epg=False):
        """Generate correct hybrid service reference"""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"🔧 _generate_hybrid_sref: dvb_sref={dvb_sref}, for_epg={for_epg}")

        # IF it's for EPG, RETURN the ORIGINAL DVB reference
        if for_epg and dvb_sref and dvb_sref.startswith('1:'):
            # ONLY FIX the namespace if necessary, but RETURN DVB
            parts = dvb_sref.split(':')
            if len(parts) >= 11 and (parts[6] == '0' or parts[6] == 'EEEE'):
                parts[6] = '820000'  # Satellite
                corrected_sref = ':'.join(parts)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"🔧 DVB NAMESPACE FIXED for EPG: {dvb_sref} -> {corrected_sref}")
                return corrected_sref
            return dvb_sref  # ⬅️ IMPORTANT: Return the DVB reference for EPG

        # CASE 1: If it's already an IPTV reference
        if dvb_sref and dvb_sref.startswith('4097:'):
            if for_epg:
                parts = dvb_sref.split(':')
                if len(parts) >= 11:
                    service_type = parts[2]
                    service_id = parts[3]
                    ts_id = parts[4]
                    on_id = parts[5]
                    namespace = parts[6]

                    dvb_reference = f"1:0:{service_type}:{service_id}:{ts_id}:{on_id}:{namespace}:0:0:0:"
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"🔧 IPTV->DVB: {dvb_sref} -> {dvb_reference}")
                    return dvb_reference
            else:
                return dvb_sref

        # CASE 2: If we have a valid DVB reference
        if dvb_sref and dvb_sref.startswith('1:'):
            if for_epg:
                # Fix namespace if necessary
                parts = dvb_sref.split(':')
                if len(parts) >= 11 and (parts[6] == '0' or parts[6] == 'EEEE'):
                    parts[6] = '820000'  # Satellite
                    corrected_sref = ':'.join(parts)
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"🔧 DVB NAMESPACE: {dvb_sref} -> {corrected_sref}")
                    return corrected_sref
                return dvb_sref
            else:
                # For bouquet, convert DVB -> IPTV
                parts = dvb_sref.split(':')
                if len(parts) >= 11:
                    service_type = parts[2]
                    service_id = parts[3]
                    ts_id = parts[4]
                    on_id = parts[5]
                    namespace = parts[6]

                    base_sref = f"4097:0:{service_type}:{service_id}:{ts_id}:{on_id}:{namespace}:0:0:0:"

                    if url:
                        encoded_url = url.replace(':', '%3a').replace(' ', '%20')
                        iptv_sref = base_sref + encoded_url
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info(f"🔧 DVB->IPTV: {dvb_sref} -> {iptv_sref}")
                        return iptv_sref

        # CASE 3: Fallback
        if url:
            if for_epg:
                url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:8]
                service_id = int(url_hash, 16) % 65536
                epg_sref = f"1:0:1:{service_id}:0:0:820000:0:0:0:"
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"🔧 EPG FALLBACK: {epg_sref}")
                return epg_sref
            else:
                # For bouquet, generate IPTV reference
                bouquet_sref = self._generate_service_reference(url)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"🔧 BOUQUET FALLBACK: {bouquet_sref}")
                return bouquet_sref

        logger.warning("❌ _generate_hybrid_sref: No valid reference generated")
        return None

    def _generate_service_reference(self, url):
        """Generate proper IPTV service reference (4097) with UNIQUE service_id"""
        if not url:
            return None

        # Generate UNIQUE service_id from URL
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:8]
        service_id = int(url_hash, 16) % 65536

        # Encode URL properly
        encoded_url = url.replace(':', '%3a')
        encoded_url = encoded_url.replace(' ', '%20')
        encoded_url = encoded_url.replace('?', '%3f')
        encoded_url = encoded_url.replace('=', '%3d')
        encoded_url = encoded_url.replace('&', '%26')
        encoded_url = encoded_url.replace('#', '%23')

        # IPTV service reference with UNIQUE service_id
        service_ref = f"4097:0:1:{service_id}:0:0:0:0:0:0:{encoded_url}"

        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"Generated IPTV service reference: {service_ref}")

        return service_ref

    def _generate_dvb_service_ref(self, channel_name, channel_id):
        """Generate a valid DVB-style service reference for EPGShare channels."""
        try:
            # Use the hash of channel_id to create a consistent service_id
            channel_hash = hashlib.md5(channel_id.encode('utf-8')).hexdigest()[:8]
            service_id = int(channel_hash, 16) % 65536

            # Build a standard DVB service reference for EPG use
            # Format: 1:0:TYPE:SERVICE_ID:TS_ID:ON_ID:NAMESPACE:0:0:0:
            return f"1:0:1:{service_id}:0:0:820000:0:0:0:"

        except Exception as e:
            logger.error(f"Error generating DVB service reference: {str(e)}")
            # Fallback to a default DVB reference
            return "1:0:1:1000:0:0:820000:0:0:0:"

    def _generate_rytec_variants(self, tvg_id, clean_name):
        """Generate multiple Rytec ID variants for matching"""
        variants = []

        if not tvg_id:
            return variants

        # Original format
        variants.append(tvg_id)
        variants.append(tvg_id.lower())
        variants.append(tvg_id.upper())

        # Common Rytec transformations
        if '.' in tvg_id:
            parts = tvg_id.split('.')
            # Remove domain suffixes
            if len(parts) > 1:
                base = ''.join(parts[:-1])
                country = parts[-1]
                variants.extend([
                    f"{base}.{country}",
                    f"{base.lower()}.{country}",
                    f"{base.upper()}.{country}",
                    base,  # Without country
                    base.lower(),
                    base.upper()
                ])

        # Clean name variants
        if clean_name:
            clean_no_spaces = clean_name.replace(' ', '').replace('.', '')
            variants.extend([
                clean_no_spaces,
                clean_no_spaces.lower(),
                clean_no_spaces.upper(),
                f"{clean_no_spaces}.it",
                f"{clean_no_spaces.lower()}.it"
            ])

        return list(set(variants))  # Remove duplicates

    def _calculate_similarity(self, name1, name2):
        """Calculate similarity between two names."""
        if not name1 or not name2:
            return 0.0

        # Convert to lowercase for case-insensitive comparison
        name1_lower = name1.lower()
        name2_lower = name2.lower()

        if name1_lower == name2_lower:
            return 1.0

        # Remove quality indicators
        name1_clean = self._quality_pattern.sub('', name1_lower)
        name2_clean = self._quality_pattern.sub('', name2_lower)

        name1_clean = name1_clean.strip()
        name2_clean = name2_clean.strip()
        if name1_clean == name2_clean:
            return 0.8

        try:
            from difflib import SequenceMatcher
            return SequenceMatcher(None, name1_clean, name2_clean).ratio()
        except ImportError:
            common_chars = set(name1_clean) & set(name2_clean)
            return len(common_chars) / max(len(set(name1_clean)), len(set(name2_clean)))

    def _get_source_type(self, comment):
        """Determine source type with greater precision."""
        if not comment:
            return 'unknown'

        comment_lower = comment.lower()

        satellite_positions = {
            '13.0e': 'hotbird', '13e': 'hotbird', '13°e': 'hotbird',
            '19.2e': 'astra19', '19e': 'astra19', '19.2': 'astra19',
            '28.2e': 'astra28', '28e': 'astra28', '28.2': 'astra28',
            '23.5e': 'astra23', '23e': 'astra23',
            '5.0w': 'amazonas', '5w': 'amazonas',
            '0.8w': 'thor', '0.8w': 'thor',
            '4.8e': 'sirius', '4.8': 'sirius',
            '7.0w': 'nilesat', '7w': 'nilesat',
            '9.0e': 'eutelsat9', '9e': 'eutelsat9'
        }

        for pos_key, pos_name in satellite_positions.items():
            if pos_key in comment_lower:
                return f'satellite_{pos_name}'

        if any(x in comment_lower for x in ['iptv', 'http', 'https', 'stream']):
            return 'iptv'
        elif any(x in comment_lower for x in ['terrestre', 'dvb-t', 'tnt', 'antenna']):
            return 'terrestrial'
        elif any(x in comment_lower for x in ['cable', 'dvbc', 'via cavo']):
            return 'cable'
        elif any(x in comment_lower for x in ['misc', 'varie', 'other']):
            return 'misc'

        return 'unknown'

    def _get_correct_epg_id(self, channel_name, tvg_id=None, service_ref=None):
        """EPG ID matching - USE ORIGINAL RYTEC CASE"""
        if tvg_id:
            # Search for the original ID in the Rytec database (case-sensitive)
            rytec_id = self._convert_to_rytec_format(tvg_id)

            # If found in Rytec, use the original ID (with correct case)
            if rytec_id in self.mapping.rytec['basic']:
                return rytec_id  # "Canale5.it" (uppercase)
            else:
                return rytec_id.lower()  # Fallback: "canale5.it"

        return self._generate_clean_rytec_id(channel_name, service_ref)

    def _get_cache_statistics(self):
        """Return accurate cache statistics with proper reset handling"""
        try:
            # Ensure counters exist
            if not hasattr(self, '_match_cache_hits'):
                self._match_cache_hits = 0
            if not hasattr(self, '_match_cache_misses'):
                self._match_cache_misses = 0

            # Calculate REAL statistics
            total_match_requests = self._match_cache_hits + self._match_cache_misses

            if total_match_requests > 0:
                match_hit_rate = (self._match_cache_hits / total_match_requests * 100)
                # Prevent unrealistic values (should be between 0-100)
                match_hit_rate = max(0, min(100, match_hit_rate))
            else:
                match_hit_rate = 0

            # Clean cache if too large (prevent memory issues)
            if len(self._match_cache) > self._cache_max_size:
                excess = len(self._match_cache) - self._cache_max_size
                keys_to_remove = list(self._match_cache.keys())[:excess]
                for key in keys_to_remove:
                    del self._match_cache[key]
                logger.debug(f"🧹 Cleaned {excess} entries from match cache")

            # Get statistics from the stats counters
            stats_counters = getattr(self, '_stats_counters', {})

            # Safely extract counts with defaults
            rytec_matches = stats_counters.get('rytec_matches', 0)
            dvb_matches = stats_counters.get('dvb_matches', 0)
            dvbt_matches = stats_counters.get('dvbt_matches', 0)
            fallback_matches = stats_counters.get('fallback_matches', 0)
            manual_db_matches = stats_counters.get('manual_db_matches', 0)

            # Calculate total processed
            total_processed = (rytec_matches + dvb_matches + dvbt_matches +
                               fallback_matches + manual_db_matches)

            # Calculate percentages safely
            if total_processed > 0:
                rytec_percent = (rytec_matches / total_processed * 100)
                dvb_percent = (dvb_matches / total_processed * 100)
                dvbt_percent = (dvbt_matches / total_processed * 100)
                fallback_percent = (fallback_matches / total_processed * 100)
                manual_percent = (manual_db_matches / total_processed * 100)
                effective_coverage = 100 - fallback_percent
            else:
                rytec_percent = dvb_percent = dvbt_percent = 0
                fallback_percent = manual_percent = effective_coverage = 0

            return {
                'match_hits': self._match_cache_hits,
                'match_misses': self._match_cache_misses,
                'match_total_requests': total_match_requests,
                'match_hit_rate': f"{match_hit_rate:.1f}%",
                'match_cache_size': len(self._match_cache),

                # Real match counts
                'rytec_matches': rytec_matches,
                'dvb_matches': dvb_matches,
                'dvbt_matches': dvbt_matches,
                'fallback_matches': fallback_matches,
                'manual_db_matches': manual_db_matches,
                'total_matches': total_processed,

                # Accurate percentages
                'rytec_percent': rytec_percent,
                'dvb_percent': dvb_percent,
                'dvbt_percent': dvbt_percent,
                'fallback_percent': fallback_percent,
                'manual_percent': manual_percent,
                'effective_coverage': effective_coverage,

                # Additional info for debugging
                'database_mode': self.database_mode,
                'total_processed': total_processed
            }
        except Exception as e:
            logger.error(f"Error in cache statistics: {str(e)}")
            return {'match_hit_rate': '0%', 'match_cache_size': 0}

    def _get_cache_statisticsOLD(self):
        """Return detailed cache statistics"""

        total_expected = (self._stats_counters.get('rytec_matches', 0) +
                          self._stats_counters.get('dvb_matches', 0) +
                          self._stats_counters.get('dvbt_matches', 0) +
                          self._stats_counters.get('fallback_matches', 0) +
                          self._stats_counters.get('manual_db_matches', 0))

        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"🔢 COUNTER VERIFICATION: Total expected: {total_expected}")

        # Calculate total match requests
        total_match_requests = self._match_cache_hits + self._match_cache_misses
        match_hit_rate = (self._match_cache_hits / total_match_requests * 100) if total_match_requests > 0 else 0

        # Calculate EPG cache statistics
        total_epg_requests = self.epg_cache_hits + self.epg_cache_misses
        epg_hit_rate = (self.epg_cache_hits / total_epg_requests * 100) if total_epg_requests > 0 else 0

        # Cache analysis
        cache_analysis = {
            'compatible': 0,
            'incompatible': 0,
            'empty': 0
        }

        # Analyze match cache content
        for key, value in self._match_cache.items():
            if not isinstance(value, dict):
                cache_analysis['empty'] += 1
                continue

            result = value.get('sref')
            if not result:
                cache_analysis['empty'] += 1
            elif self._is_service_compatible(result):
                cache_analysis['compatible'] += 1
            else:
                cache_analysis['incompatible'] += 1

        # Channel cache statistics
        channel_cache_hits = getattr(self, '_channel_cache_hits', 0)
        channel_cache_misses = getattr(self, '_channel_cache_misses', 0)
        channel_cache_size = len(getattr(self, '_clean_name_cache', {}))

        # Rytec channels count
        rytec_channels_count = len(self.mapping.rytec.get('basic', {}))

        # Manual DB statistics
        manual_db_data = self.manual_db.load_database()
        manual_db_size = len(manual_db_data.get('mappings', []))
        manual_db_enabled = config.plugins.m3uconverter.use_manual_database.value

        # Use REAL counters from statistics
        stats_counters = getattr(self, '_stats_counters', {})

        # Safely extract counts
        rytec_matches = stats_counters.get('rytec_matches', 0)
        dvb_matches = stats_counters.get('dvb_matches', 0)
        dvbt_matches = stats_counters.get('dvbt_matches', 0)
        fallback_matches = stats_counters.get('fallback_matches', 0)
        manual_db_matches = stats_counters.get('manual_db_matches', 0)

        # Calculate TOTAL matches correctly - sum of ALL types
        total_matches = (rytec_matches + dvb_matches + dvbt_matches +
                         fallback_matches + manual_db_matches)

        # Calculate REAL EPG matches (excluding fallback)
        real_epg_matches = (rytec_matches + dvb_matches + dvbt_matches + manual_db_matches)

        # Calculate percentages safely
        if total_matches > 0:
            rytec_percent = (rytec_matches / total_matches * 100)
            dvb_percent = (dvb_matches / total_matches * 100)
            dvbt_percent = (dvbt_matches / total_matches * 100)
            fallback_percent = (fallback_matches / total_matches * 100)
            manual_percent = (manual_db_matches / total_matches * 100)
            epg_coverage = (real_epg_matches / total_matches * 100)
        else:
            rytec_percent = dvb_percent = dvbt_percent = 0
            fallback_percent = manual_percent = epg_coverage = 0

        # Calculate effective EPG matches based on database mode
        if self.database_mode == "full":
            effective_epg_matches = rytec_matches + dvb_matches + dvbt_matches + manual_db_matches
        elif self.database_mode == "both":
            effective_epg_matches = rytec_matches + dvb_matches + manual_db_matches
        elif self.database_mode == "dvb":
            effective_epg_matches = dvb_matches + manual_db_matches
        elif self.database_mode == "rytec":
            effective_epg_matches = rytec_matches + manual_db_matches
        elif self.database_mode == "dtt":
            effective_epg_matches = dvbt_matches + manual_db_matches
        else:
            effective_epg_matches = manual_db_matches

        effective_coverage = (effective_epg_matches / total_matches * 100) if total_matches > 0 else 0

        # Enabled flags
        rytec_enabled = self.database_mode in ["full", "both", "rytec"]
        dvb_enabled = self.database_mode in ["full", "both", "dvb"]
        dvbt_enabled = self.database_mode in ["full", "dtt"]

        return {
            # Match Cache Statistics
            'match_hits': self._match_cache_hits,
            'match_misses': self._match_cache_misses,
            'match_total_requests': total_match_requests,
            'match_hit_rate': f"{match_hit_rate:.1f}%",
            'match_cache_size': len(self._match_cache),

            # EPG Cache Statistics
            'epg_hits': self.epg_cache_hits,
            'epg_misses': self.epg_cache_misses,
            'epg_total_requests': total_epg_requests,
            'epg_hit_rate': f"{epg_hit_rate:.1f}%",
            'epg_cache_size': len(self.epg_cache),

            # Combined Statistics
            'total_hits': self._match_cache_hits + self.epg_cache_hits,
            'total_misses': self._match_cache_misses + self.epg_cache_misses,
            'total_requests': total_match_requests + total_epg_requests,
            'overall_hit_rate': f"{((self._match_cache_hits + self.epg_cache_hits) / (total_match_requests + total_epg_requests) * 100) if (total_match_requests + total_epg_requests) > 0 else 0:.1f}%",

            # Cache Analysis
            'cache_analysis': cache_analysis,
            'incompatible_matches': self._incompatible_matches,
            'loaded_dvb_channels': len(self.mapping.dvb),
            'rytec_channels': rytec_channels_count,

            # Channel Cache Statistics
            'channel_cache_hits': channel_cache_hits,
            'channel_cache_misses': channel_cache_misses,
            'channel_cache_size': channel_cache_size,

            # Manual Database Statistics
            'manual_db_matches': manual_db_matches,
            'manual_db_size': manual_db_size,
            'manual_db_enabled': manual_db_enabled,

            # Match Type Statistics
            'dvbt_matches': dvbt_matches,
            'rytec_matches': rytec_matches,
            'dvb_matches': dvb_matches,
            'fallback_matches': fallback_matches,
            'total_matches': total_matches,
            'real_epg_matches': real_epg_matches,
            'match_coverage': f"{epg_coverage:.1f}%",

            # Database Mode Statistics
            'database_mode': self.database_mode,
            'effective_epg_matches': effective_epg_matches,
            'effective_coverage': f"{effective_coverage:.1f}%",

            # Enabled flags
            'rytec_enabled': rytec_enabled,
            'dvb_enabled': dvb_enabled,
            'dvbt_enabled': dvbt_enabled,

            # Percentage breakdowns
            'rytec_percent': rytec_percent,
            'dvb_percent': dvb_percent,
            'dvbt_percent': dvbt_percent,
            'fallback_percent': fallback_percent,
            'manual_percent': manual_percent,
            'epg_coverage': epg_coverage
        }

    def _get_system_country_code(self):
        """Get country code from plugin configuration with fallbacks."""
        try:
            if (hasattr(config.plugins, 'm3uconverter') and
                    hasattr(config.plugins.m3uconverter, 'language')):
                lang_setting = config.plugins.m3uconverter.language.value
                if lang_setting != 'all':
                    return lang_setting.lower()

            settings_path = "/etc/enigma2/settings"
            if fileExists(settings_path):
                with open(settings_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith('config.misc.country='):
                            country_code = line.split('=', 1)[1].strip()
                            return country_code.lower()

            timezone = time.tzname[0] if time.tzname else ''
            if 'CET' in timezone or 'CEST' in timezone:
                return 'it'

            return 'eu'

        except Exception as e:
            logger.error(f"Error getting country code: {str(e)}")
            return 'eu'

    def _get_country_from_service_ref(self, service_ref):
        if not service_ref:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug("❌ No service_ref for country detection")
            return None

        logger.debug(f"🔍 Country detection for: {service_ref}")

        # USE THE EXISTING FUNCTION FROM THE PLUGIN
        # instead of making wrong assumptions about countries
        # If the service is compatible with the system, it’s fine
        # The user determines the country from the settings
        if self._is_service_compatible(service_ref):
            return config.plugins.m3uconverter.language.value

        return ""

    def _get_epg_url_for_language(self, language_code):
        """Return the correct EPG URL based on language selection."""
        country_code = LANGUAGE_TO_COUNTRY.get(language_code, 'ALL')

        epg_urls = {
            'AL': 'https://epgshare01.online/epgshare01/epg_ripper_AL1.xml.gz',
            'ALL': 'https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz',
            'AR': 'https://epgshare01.online/epgshare01/epg_ripper_AR1.xml.gz',
            'AS': 'https://epgshare01.online/epgshare01/epg_ripper_ASIANTELEVISION1.xml.gz',
            'AT': 'https://epgshare01.online/epgshare01/epg_ripper_AT1.xml.gz',
            'AU': 'https://epgshare01.online/epgshare01/epg_ripper_AU1.xml.gz',
            'BA': 'https://epgshare01.online/epgshare01/epg_ripper_BA1.xml.gz',
            'BE': 'https://epgshare01.online/epgshare01/epg_ripper_BE2.xml.gz',
            'BEIN': 'https://epgshare01.online/epgshare01/epg_ripper_BEIN1.xml.gz',
            'BG': 'https://epgshare01.online/epgshare01/epg_ripper_BG1.xml.gz',
            'BR1': 'https://epgshare01.online/epgshare01/epg_ripper_BR1.xml.gz',
            'BR2': 'https://epgshare01.online/epgshare01/epg_ripper_BR2.xml.gz',
            'CA2': 'https://epgshare01.online/epgshare01/epg_ripper_CA2.xml.gz',
            'CH': 'https://epgshare01.online/epgshare01/epg_ripper_CH1.xml.gz',
            'CL1': 'https://epgshare01.online/epgshare01/epg_ripper_CL1.xml.gz',
            'CO1': 'https://epgshare01.online/epgshare01/epg_ripper_CO1.xml.gz',
            'CR1': 'https://epgshare01.online/epgshare01/epg_ripper_CR1.xml.gz',
            'CY': 'https://epgshare01.online/epgshare01/epg_ripper_CY1.xml.gz',
            'CZ': 'https://epgshare01.online/epgshare01/epg_ripper_CZ1.xml.gz',
            'DE': 'https://epgshare01.online/epgshare01/epg_ripper_DE1.xml.gz',
            'DK': 'https://epgshare01.online/epgshare01/epg_ripper_DK1.xml.gz',
            'DO1': 'https://epgshare01.online/epgshare01/epg_ripper_DO1.xml.gz',
            'EC1': 'https://epgshare01.online/epgshare01/epg_ripper_EC1.xml.gz',
            'EG1': 'https://epgshare01.online/epgshare01/epg_ripper_EG1.xml.gz',
            'ES': 'https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz',
            'FI': 'https://epgshare01.online/epgshare01/epg_ripper_FI1.xml.gz',
            'FR': 'https://epgshare01.online/epgshare01/epg_ripper_FR1.xml.gz',
            'GR': 'https://epgshare01.online/epgshare01/epg_ripper_GR1.xml.gz',
            'HK1': 'https://epgshare01.online/epgshare01/epg_ripper_HK1.xml.gz',
            'HR': 'https://epgshare01.online/epgshare01/epg_ripper_HR1.xml.gz',
            'HU': 'https://epgshare01.online/epgshare01/epg_ripper_HU1.xml.gz',
            'ID1': 'https://epgshare01.online/epgshare01/epg_ripper_ID1.xml.gz',
            'IL1': 'https://epgshare01.online/epgshare01/epg_ripper_IL1.xml.gz',
            'IN1': 'https://epgshare01.online/epgshare01/epg_ripper_IN1.xml.gz',
            'IN2': 'https://epgshare01.online/epgshare01/epg_ripper_IN2.xml.gz',
            'IN4': 'https://epgshare01.online/epgshare01/epg_ripper_IN4.xml.gz',
            'IT': 'https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz',
            'JM1': 'https://epgshare01.online/epgshare01/epg_ripper_JM1.xml.gz',
            'JP1': 'https://epgshare01.online/epgshare01/epg_ripper_JP1.xml.gz',
            'JP2': 'https://epgshare01.online/epgshare01/epg_ripper_JP2.xml.gz',
            'KE1': 'https://epgshare01.online/epgshare01/epg_ripper_KE1.xml.gz',
            'KR1': 'https://epgshare01.online/epgshare01/epg_ripper_KR1.xml.gz',
            'LT': 'https://epgshare01.online/epgshare01/epg_ripper_LT1.xml.gz',
            'LV': 'https://epgshare01.online/epgshare01/epg_ripper_LV1.xml.gz',
            'MN1': 'https://epgshare01.online/epgshare01/epg_ripper_MN1.xml.gz',
            'MT': 'https://epgshare01.online/epgshare01/epg_ripper_MT1.xml.gz',
            'MX1': 'https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz',
            'MY1': 'https://epgshare01.online/epgshare01/epg_ripper_MY1.xml.gz',
            'NG1': 'https://epgshare01.online/epgshare01/epg_ripper_NG1.xml.gz',
            'NL': 'https://epgshare01.online/epgshare01/epg_ripper_NL1.xml.gz',
            'NO': 'https://epgshare01.online/epgshare01/epg_ripper_NO1.xml.gz',
            'NZ1': 'https://epgshare01.online/epgshare01/epg_ripper_NZ1.xml.gz',
            'PA1': 'https://epgshare01.online/epgshare01/epg_ripper_PA1.xml.gz',
            'PE1': 'https://epgshare01.online/epgshare01/epg_ripper_PE1.xml.gz',
            'PH1': 'https://epgshare01.online/epgshare01/epg_ripper_PH1.xml.gz',
            'PH2': 'https://epgshare01.online/epgshare01/epg_ripper_PH2.xml.gz',
            'PK1': 'https://epgshare01.online/epgshare01/epg_ripper_PK1.xml.gz',
            'PL': 'https://epgshare01.online/epgshare01/epg_ripper_PL1.xml.gz',
            'PT': 'https://epgshare01.online/epgshare01/epg_ripper_PT1.xml.gz',
            'RO': 'https://epgshare01.online/epgshare01/epg_ripper_RO1.xml.gz',
            'RS': 'https://epgshare01.online/epgshare01/epg_ripper_RS1.xml.gz',
            'SA1': 'https://epgshare01.online/epgshare01/epg_ripper_SA1.xml.gz',
            'SA2': 'https://epgshare01.online/epgshare01/epg_ripper_SA2.xml.gz',
            'SE': 'https://epgshare01.online/epgshare01/epg_ripper_SE1.xml.gz',
            'SG1': 'https://epgshare01.online/epgshare01/epg_ripper_SG1.xml.gz',
            'SK': 'https://epgshare01.online/epgshare01/epg_ripper_SK1.xml.gz',
            'TH1': 'https://epgshare01.online/epgshare01/epg_ripper_TH1.xml.gz',
            'TR': 'https://epgshare01.online/epgshare01/epg_ripper_TR1.xml.gz',
            'UK': 'https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz',
            'US2': 'https://epgshare01.online/epgshare01/epg_ripper_US2.xml.gz',
            'US_LOCALS1': 'https://epgshare01.online/epgshare01/epg_ripper_US_LOCALS1.xml.gz',
            'US_SPORTS1': 'https://epgshare01.online/epgshare01/epg_ripper_US_SPORTS1.xml.gz',
            'UY1': 'https://epgshare01.online/epgshare01/epg_ripper_UY1.xml.gz',
            'VN1': 'https://epgshare01.online/epgshare01/epg_ripper_VN1.xml.gz',
            'ZA1': 'https://epgshare01.online/epgshare01/epg_ripper_ZA1.xml.gz',
            # Special Network
            'RAKUTEN1': 'https://epgshare01.online/epgshare01/epg_ripper_RAKUTEN1.xml.gz',
            'PLEX1': 'https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz',
            'DISTROTV1': 'https://epgshare01.online/epgshare01/epg_ripper_DISTROTV1.xml.gz',
            'FANDUEL1': 'https://epgshare01.online/epgshare01/epg_ripper_FANDUEL1.xml.gz',
            'DRAFTKINGS1': 'https://epgshare01.online/epgshare01/epg_ripper_DRAFTKINGS1.xml.gz',
            'POWERNATION1': 'https://epgshare01.online/epgshare01/epg_ripper_POWERNATION1.xml.gz',
            'PEACOCK1': 'https://epgshare01.online/epgshare01/epg_ripper_PEACOCK1.xml.gz',
            'TBNPLUS1': 'https://epgshare01.online/epgshare01/epg_ripper_TBNPLUS1.xml.gz',
            'THESPORTPLUS1': 'https://epgshare01.online/epgshare01/epg_ripper_THESPORTPLUS1.xml.gz',
            'RALLY_TV1': 'https://epgshare01.online/epgshare01/epg_ripper_RALLY_TV1.xml.gz',
            'SPORTKLUB1': 'https://epgshare01.online/epgshare01/epg_ripper_SPORTKLUB1.xml.gz',
            'VOA1': 'https://epgshare01.online/epgshare01/epg_ripper_VOA1.xml.gz',
            'ALJAZEERA1': 'https://epgshare01.online/epgshare01/epg_ripper_ALJAZEERA1.xml.gz',
            'VIVA_RUSSIA': 'https://epgshare01.online/epgshare01/epg_ripper_viva-russia.ru.xml.gz',
            'DELUXEMUSIC1': 'https://epgshare01.online/epgshare01/epg_ripper_DELUXEMUSIC1.xml.gz',
            'DIRECTVSPORTS1': 'https://epgshare01.online/epgshare01/epg_ripper_DIRECTVSPORTS1.xml.gz',
            'DUMMY_CHANNELS': 'https://epgshare01.online/epgshare01/epg_ripper_DUMMY_CHANNELS.xml.gz',
            'PAC-12': 'https://epgshare01.online/epgshare01/epg_ripper_PAC-12.xml.gz',
            'SSPORTPLUS1': 'https://epgshare01.online/epgshare01/epg_ripper_SSPORTPLUS1.xml.gz',
            'LOCOMOTIONTV': 'https://epgshare01.online/epgshare01/locomotiontv.xml.gz'
        }

        return epg_urls.get(country_code, epg_urls['ALL'])

    def _download_epg_file(self, url, output_path):
        """Download and decompress EPG file."""
        try:
            import requests
            import gzip
            from io import BytesIO
            headers = {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "application/xml, */*"
            }

            # Download the file
            response = requests.get(url, headers=headers, timeout=30, verify=False)
            response.raise_for_status()

            # Check if it's a gzipped file
            if url.endswith('.gz') or response.headers.get('content-type') == 'application/gzip':
                # Decompress gzip file
                with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz_file:
                    decompressed_content = gz_file.read()

                # Write decompressed content
                with open(output_path, 'wb') as f:
                    f.write(decompressed_content)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"Downloaded and decompressed EPG file to {output_path}")

            else:
                # Write directly if not compressed
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"Downloaded EPG file to {output_path}")

            return True

        except Exception as e:
            logger.error(f"EPG download failed: {str(e)}")
            return False

    def _download_and_parse_epgshare(self, language_code="all"):
        """Download and parse EPGShare data with extensive debugging."""
        try:
            country_code = LANGUAGE_TO_COUNTRY.get(language_code, 'ALL')
            epg_url = f"https://epgshare01.online/epgshare01/epg_ripper_{country_code}1.xml.gz"
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Downloading EPG from: {epg_url}")

            # Download the file
            temp_path = join(LOG_DIR, "epgshare_download.xml")
            success = self._download_epg_file(epg_url, temp_path)

            if not success:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("EPGShare download failed")
                return False

            # Parse the file
            parse_success = self._parse_epgshare_for_mapping(temp_path)

            if parse_success:
                # Verify results
                epgshare_count = 0
                with self._rytec_lock:
                    for channel_id, variants in self.mapping.rytec['extended'].items():
                        for variant in variants:
                            if variant.get('source_type') == 'epgshare':
                                epgshare_count += 1
                                break
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"EPGShare parsing completed: {epgshare_count} channels")

                if epgshare_count > 0:
                    return True
                else:
                    logger.error("Parsing succeeded but no channels were added!")
                    return False
            else:
                logger.error("EPGShare parsing failed")
                return False

        except Exception as e:
            logger.error(f"EPG Share error: {str(e)}")
            return False

    def _create_fallback_mapping_from_dvb(self):
        """Create fallback EPG mapping from existing DVB services."""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info("Creating fallback mapping from DVB services...")

        count = 0
        with self._rytec_lock:
            for clean_name, services in self.mapping.dvb.items():
                if services and count < 1000:  # Limit to 1000 channels
                    service = services[0]
                    if service['sref'] and self._is_service_compatible(service['sref']):
                        # Generate Rytec-style ID
                        channel_id = self._generate_rytec_style_id(clean_name, service['sref'])

                        self.mapping.rytec['extended'][channel_id].append({
                            'sref': service['sref'],
                            'channel_name': clean_name,
                            'source_type': 'dvb_fallback',
                            'original_id': channel_id
                        })

                        self.mapping.rytec['basic'][channel_id] = service['sref']
                        count += 1

        logger.info(f"Created fallback mapping with {count} DVB channels")

    def _save_good_matches(self, conversion_data):
        """Save good matches in BATCH mode - ONLY ONCE"""
        try:
            if not config.plugins.m3uconverter.use_manual_database.value:
                return 0

            def background_auto_save():
                try:
                    mappings_to_save = []
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info("Background auto-save: Collecting mappings...")

                    for channel in conversion_data:
                        match_type = channel.get('match_type', '')
                        service_ref = channel.get('original_service_ref', '')
                        channel_name = channel.get('name', '')
                        tvg_id = channel.get('tvg_id', '')

                        # Save only DVB references with EPG
                        if (service_ref and
                                service_ref.startswith('1:0:') and  # DVB only
                                any(epg_type in match_type for epg_type in ['rytec', 'dvb', 'dvbt'])):

                            clean_name = self.clean_channel_name(channel_name)

                            mapping_data = {
                                'channel_name': channel_name,
                                'original_name': channel.get('original_name', channel_name),
                                'clean_name': clean_name,
                                'tvg_id': tvg_id,
                                'assigned_sref': service_ref,
                                'match_type': "auto_saved_rytec" if 'rytec' in match_type else
                                              "auto_saved_dvbt" if 'dvbt' in match_type else
                                              "auto_saved_dvb" if 'dvb' in match_type else
                                              "auto_saved_epg",
                                'similarity': 1.0,
                                'bouquet_source': 'auto_save',
                                'created': strftime("%Y-%m-%d %H:%M:%S"),
                                'last_used': strftime("%Y-%m-%d %H:%M:%S")
                            }
                            mappings_to_save.append(mapping_data)

                    # SAVE EVERYTHING IN A SINGLE OPERATION
                    if mappings_to_save:
                        success = self._save_auto_mappings_batch(mappings_to_save)
                        if success:
                            logger.info(f"💾 AUTO-SAVE BATCH: {len(mappings_to_save)} mappings saved")
                        else:
                            logger.error("❌ Auto-save batch failed")
                    else:
                        logger.info("ℹ️ No mappings to save in auto-save")

                except Exception as e:
                    logger.error(f"Background auto-save error: {str(e)}")

            # Start thread - every save is immediate and thread-safe
            thread = threading.Thread(target=background_auto_save, daemon=True)
            thread.start()

            return 0

        except Exception as e:
            logger.error(f"Auto-save setup error: {str(e)}")
            return 0

    def _save_auto_mappings_batch(self, mappings_to_save):
        """Save a list of mappings in a single operation"""
        try:
            if not hasattr(self, 'manual_db') or not self.manual_db:
                logger.error("❌ Manual database not available for batch save")
                return False

            # Load current database
            data = self.manual_db.load_database()
            current_mappings = data.get('mappings', [])

            # Create a dictionary for fast lookup
            mapping_dict = {m.get('clean_name', '').lower(): m for m in current_mappings}

            # Add/update new mappings
            for new_mapping in mappings_to_save:
                clean_name = new_mapping.get('clean_name', '').lower()
                if clean_name:
                    # Remove original_sref if it exists
                    if 'original_sref' in new_mapping:
                        del new_mapping['original_sref']
                    mapping_dict[clean_name] = new_mapping

            # Convert back to list
            data['mappings'] = list(mapping_dict.values())
            data['last_updated'] = strftime("%Y-%m-%d %H:%M:%S")

            # Save everything at once
            success = self.manual_db.save_database(data)

            if success:
                logger.info(f"✅ Batch save successful: {len(mappings_to_save)} mappings")
            else:
                logger.error("❌ Batch save failed")

            return success

        except Exception as e:
            logger.error(f"❌ Batch save error: {str(e)}")
            return False

    def normalize_conversion_data(self, conversion_data):
        """Convert all channel data to consistent dictionary format"""
        normalized_data = []

        for channel in conversion_data:
            if isinstance(channel, dict):
                normalized_data.append(channel)
            elif isinstance(channel, (tuple, list)) and len(channel) >= 2:
                # Convert tuple (name, url) to dictionary
                normalized_data.append({
                    'name': channel[0],
                    'url': channel[1],
                    'original_name': channel[0],
                    'match_type': 'tv_bouquet'
                })
            else:
                # Skip or log invalid entries
                logger.warning(f"Invalid channel data format: {type(channel)}")

        return normalized_data

    def _save_quick_debug(self, conversion_data, bouquet_name):
        """Save lightweight debug only when debug mode is enabled"""
        if not config.plugins.m3uconverter.enable_debug.value:
            return

        try:
            normalized_data = self.normalize_conversion_data(conversion_data)
            makedirs(DEBUG_DIR, exist_ok=True)
            # 1. First clean old file
            self._cleanup_old_debug_files()
            timestamp = strftime("%Y%m%d_%H%M%S")
            import random
            unique_id = random.randint(1000, 9999)
            debug_file_tab = join(DEBUG_DIR, f"{timestamp}_{unique_id}_{bouquet_name}_quick_tab.csv")

            # 2. Also create a TAB-friendly version
            with open(debug_file_tab, 'w', encoding='utf-8') as f:
                separator = ';'  # Excel-friendly delimiter
                f.write(f"Channel{separator}Original_Name{separator}TVG_ID{separator}Clean_Name{separator}Match_Type{separator}Has_EPG{separator}Service_Ref{separator}URL_Start\n")

                for channel in normalized_data:
                    # CORREZIONE: Gestire sia dizionari che tuple
                    if isinstance(channel, dict):
                        # Caso dizionario (M3U, JSON)
                        name = channel.get('name', 'Unknown')[:50]
                        original_name = channel.get('original_name', name)
                        tvg_id = channel.get('tvg_id', '')
                        clean_name = self.clean_channel_name(name) if hasattr(self, 'clean_channel_name') else name
                        match_type = channel.get('match_type', '')
                        has_epg = 'YES' if any(x in match_type for x in ['rytec', 'dvb', 'dvbt']) else 'NO'
                        service_ref = channel.get('sref', '')[:50]
                        url = channel.get('url', '')[:30]

                    elif isinstance(channel, (tuple, list)) and len(channel) >= 2:
                        # Caso tuple (TV bouquets) - formato (nome, url)
                        name = str(channel[0])[:50] if len(channel) > 0 else 'Unknown'
                        original_name = name
                        tvg_id = ''
                        clean_name = self.clean_channel_name(name) if hasattr(self, 'clean_channel_name') else name
                        match_type = 'tv_bouquet'
                        has_epg = 'NO'  # I bouquet TV di solito non hanno EPG integrato
                        service_ref = ''
                        url = str(channel[1])[:30] if len(channel) > 1 else ''

                    else:
                        # Caso sconosciuto - usare valori di default
                        name = str(channel)[:50]
                        original_name = name
                        tvg_id = ''
                        clean_name = name
                        match_type = 'unknown'
                        has_epg = 'NO'
                        service_ref = ''
                        url = ''

                    f.write(f"{name}{separator}{original_name}{separator}{tvg_id}{separator}{clean_name}{separator}{match_type}{separator}{has_epg}{separator}{service_ref}{separator}{url}\n")

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"📁 Debug CSV saved: {debug_file_tab}")

        except Exception as e:
            logger.error(f"❌ Quick debug saving error: {str(e)}")

    def _save_complete_cache_analysis(self, output_dir=join(LOG_DIR, "epg_analysis")):
        """Save complete cache and database analysis"""
        try:
            if not exists(output_dir):
                makedirs(output_dir)

            # Cache statistics
            cache_stats = self._get_cache_statistics()
            with open(join(output_dir, "cache_stats.json"), 'w') as f:
                json.dump(cache_stats, f, indent=2, ensure_ascii=False)

            # Database summary
            db_summary = {
                'rytec_basic_count': len(self.mapping.rytec['basic']),
                'rytec_extended_count': len(self.mapping.rytec['extended']),
                'dvb_channels_count': len(self.mapping.dvb),
                'optimized_count': len(self.mapping.optimized)
            }

            with open(join(output_dir, "database_summary.json"), 'w') as f:
                json.dump(db_summary, f, indent=2, ensure_ascii=False)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Complete analysis saved in: {output_dir}")
            return True

        except Exception as e:
            logger.error(f"Error saving analysis: {str(e)}")
            return False

    def _debug_verify_epg_files(self, bouquet_name):
        """Verify that EPG files were created correctly."""
        epgimport_path = "/etc/epgimport"
        channels_file = join(epgimport_path, f"{bouquet_name}.channels.xml")
        sources_file = join(epgimport_path, "ArchimedeConverter.sources.xml")
        channels_exists = fileExists(channels_file)
        sources_exists = fileExists(sources_file)

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info("=== EPG FILE VERIFICATION ===")
            logger.info(f"Channels file: {channels_exists} - {channels_file}")
            logger.info(f"Sources file: {sources_exists} - {sources_file}")

        # Verify content
        channel_count = 0
        if channels_exists:
            try:
                with open(channels_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    channel_count = content.count('<channel id=')
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"Channels in file: {channel_count}")

                    # Count match types
                    rytec_matches = content.count('rytec_')
                    dvb_matches = content.count('dvb_')
                    fallback_matches = content.count('iptv_fallback')

                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"Rytec matches: {rytec_matches}, DVB matches: {dvb_matches}, Fallback: {fallback_matches}")

            except Exception as e:
                logger.error(f"Error reading channels.xml: {str(e)}")

        # Verify sources
        if sources_exists:
            try:
                with open(sources_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    has_bouquet = bouquet_name in content
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"Bouquet present in sources: {has_bouquet}")

            except Exception as e:
                logger.error(f"Error reading sources.xml: {str(e)}")

        return channels_exists and sources_exists and channel_count > 0

    def _debug_matching_process(self, original_name, clean_name, tvg_id, result, match_type):
        """Debug dettagliato del processo di matching."""
        if not config.plugins.m3uconverter.enable_debug.value:
            return

        logger.debug(f"=== MATCHING DEBUG for: {original_name} ===")
        logger.debug(f"Clean name: {clean_name}")
        logger.debug(f"TVG ID: {tvg_id}")
        logger.debug(f"Final EPG ID: {self._get_correct_epg_id(original_name, tvg_id, result)}")
        logger.debug(f"Result: {result}")
        logger.debug(f"Match type: {match_type}")

    def _cleanup_all_match_types(self):
        """Global cleanup of all match_type entries in the system"""
        try:
            # 1. Clean cache
            cleaned_cache = 0
            for key, value in self._match_cache.items():
                if isinstance(value, dict) and 'match_type' in value:
                    old_type = value['match_type']
                    new_type = self._clean_single_match_type(old_type)
                    if new_type != old_type:
                        value['match_type'] = new_type
                        cleaned_cache += 1

            # 2. Clean manual database - LOAD FIRST, MODIFY, THEN SAVE ONCE
            # manual_db = ManualDatabaseManager()
            data = self.manual_db.load_database()
            cleaned_db = 0

            for mapping in data.get('mappings', []):
                old_type = mapping.get('match_type', '')
                new_type = self._clean_single_match_type(old_type)
                if new_type != old_type:
                    mapping['match_type'] = new_type
                    cleaned_db += 1

            # SAVE ONLY IF THERE ARE CHANGES, AND DO IT ONCE
            if cleaned_db > 0:
                self.manual_db.save_database(data)  # The manager will handle backup intelligently

            # 3. Clean conversion_data if it exists
            cleaned_conv = 0
            if hasattr(self, 'conversion_data'):
                for channel in self.conversion_data:
                    old_type = channel.get('match_type', '')
                    new_type = self._clean_single_match_type(old_type)
                    if new_type != old_type:
                        channel['match_type'] = new_type
                        cleaned_conv += 1
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"🧹 GLOBAL CLEAN: cache={cleaned_cache}, db={cleaned_db}, conv={cleaned_conv}")

            return True

        except Exception as e:
            logger.error(f"❌ Global cleanup error: {str(e)}")
            return False

    def _clean_single_match_type(self, match_type):
        """Clean a single match_type value"""
        if not match_type or not isinstance(match_type, str):
            return 'auto'

        # Keep it if already simple
        if match_type in ['manual_rytec', 'manual_dvb', 'manual_dvbt', 'manual_db',
                          'rytec_exact', 'rytec_conv', 'dvb_s', 'dvb_t', 'iptv_fallback']:
            return match_type

        # Otherwise, normalize it
        parts = match_type.split('_')

        if 'manual' in parts:
            if 'rytec' in parts:
                return 'manual_rytec'
            elif 'dvb' in parts:
                if 't' in parts or 'dvbt' in parts:
                    return 'manual_dvbt'
                else:
                    return 'manual_dvb'
            else:
                return 'manual_db'
        elif 'rytec' in parts:
            return 'rytec_auto'
        elif 'dvb' in parts:
            if 't' in parts or 'dvbt' in parts:
                return 'dvb_t'
            else:
                return 'dvb_s'
        else:
            return 'auto'

    def _cleanup_smart(self):
        """Remove temporary debug files older than 1 day."""
        try:
            cleanup_patterns = [
                join(DEBUG_DIR, "*_quick.csv"),
                join(DEBUG_DIR, "*_quick_tab.csv"),
            ]

            cleaned_count = 0
            cutoff_time = time.time() - 86400  # 1 day = 24 * 3600 seconds

            for pattern in cleanup_patterns:
                for filepath in glob.glob(pattern):
                    try:
                        if getmtime(filepath) < cutoff_time:
                            remove(filepath)
                            cleaned_count += 1
                    except Exception:
                        pass

            if cleaned_count > 0:
                logger.info(f"🧹 Cleaned {cleaned_count} debug files older than 1 day.")
            else:
                logger.debug("No old debug files found to clean.")

        except Exception as e:
            logger.debug(f"Cleanup skipped due to error: {str(e)}")

    def _cleanup_log_file(self):
        """Completely clear the log file before new conversion"""
        try:
            if exists(self.log_file):
                file_size = getsize(self.log_file) if exists(self.log_file) else 0
                if file_size > 1024 * 1024:
                    with open(self.log_file, 'w', encoding='utf-8') as f:
                        f.write('')
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info("Log file cleared (was too large)")
        except Exception as e:
            logger.error(f"Log cleanup error: {str(e)}")

    def _cleanup_old_debug_files(self, max_files=3):
        """Keep only the last N debug files"""
        try:
            # Pattern file debug
            patterns = ["*_quick_tab.csv", "*_quick.csv"]

            for pattern in patterns:
                files = sorted(glob.glob(join(DEBUG_DIR, pattern)),
                               key=lambda x: getmtime(x) if exists(x) else 0)

                for old_file in files[:-max_files]:
                    try:
                        remove(old_file)
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.debug(f"🧹 Removed old debug: {basename(old_file)}")
                    except Exception as e:
                        logger.debug(f"Could not remove {old_file}: {e}")

        except Exception as e:
            logger.debug(f"Debug cleanup skipped: {e}")


class ConversionSelector(Screen):
    """Main conversion selector screen."""
    if SCREEN_WIDTH > 1280:
        skin = """
            <screen name="ConversionSelector" position="center,center" size="1920,1080" title="..::ConversionSelector::.." backgroundColor="#20000000" flags="wfNoBorder">
                <eLabel backgroundColor="#002d3d5b" cornerRadius="30" position="0,0" size="1920,1080" zPosition="-2" />
                <widget source="Title" render="Label" position="38,12" size="1680,78" font="Regular; 36" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
                <widget name="list" position="38,90" size="1260,777" itemHeight="60" font="Regular;42" scrollbarMode="showNever" />
                <widget name="status" position="35,912" size="1778,75" font="Regular;42" backgroundColor="background" transparent="1" foregroundColor="white" />
                <eLabel name="" position="1830,986" size="78,78" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="14" font="Regular; 24" zPosition="1" text="MENU" />
                <widget source="session.CurrentService" render="Label" position="1308,81" size="600,51" font="Regular;42" borderWidth="1" backgroundColor="background" transparent="1" halign="center" foregroundColor="white" zPosition="30" valign="center" noWrap="1">
                    <convert type="ServiceName">Name</convert>
                </widget>
                <widget source="session.VideoPicture" render="Pig" position="1307,138" zPosition="20" size="600,330" backgroundColor="transparent" transparent="0" cornerRadius="21" />
                <widget name="info" position="0,0" size="1,1" font="Regular;1" transparent="1" />
                <widget name="text" position="0,0" size="1,1" font="Regular;1" transparent="1" />
                <!--#####red####/-->
                <eLabel backgroundColor="#00ff0000" position="38,1050" size="375,9" zPosition="12" />
                <widget name="key_red" position="38,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
                <widget source="key_red" render="Label" position="38,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
                <!--#####green####/-->
                <eLabel backgroundColor="#0000ff00" position="420,1050" size="375,9" zPosition="12" />
                <widget name="key_green" position="420,990" size="375,68" zPosition="11" font="Regular;32" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
                <widget source="key_green" render="Label" position="420,990" size="375,68" zPosition="11" font="Regular;32" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
                <!--#####yellow####/-->
                <eLabel backgroundColor="#00ffff00" position="812,1050" size="375,9" zPosition="12" />
                <widget name="key_yellow" position="808,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
                <widget source="key_yellow" render="Label" position="808,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
                <!--#####blue####/-->
                <eLabel backgroundColor="#000000ff" position="1197,1050" size="375,9" zPosition="12" />
                <widget name="key_blue" position="1196,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,Button,Label" transparent="1" />
                <widget source="key_blue" render="Label" position="1196,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,StaticText" transparent="1" />
            </screen>"""

    else:
        skin = """
            <screen name="ConversionSelector" position="center,center" size="1280,720" title="..::ConversionSelector::.." backgroundColor="#20000000" flags="wfNoBorder">
                <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1280,720" zPosition="-2" />
                <widget source="Title" render="Label" position="25,8" size="1120,52" font="Regular; 24" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
                <widget name="list" position="25,60" size="840,518" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
                <widget name="status" position="23,608" size="1185,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
                <eLabel name="" position="1220,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="MENU" />
                <widget source="session.CurrentService" render="Label" position="872,54" size="400,34" font="Regular;26" borderWidth="1" backgroundColor="background" transparent="1" halign="center" foregroundColor="white" zPosition="30" valign="center" noWrap="1">
                    <convert type="ServiceName">Name</convert>
                </widget>
                <widget source="session.VideoPicture" render="Pig" position="871,92" zPosition="20" size="400,220" backgroundColor="transparent" transparent="0" cornerRadius="14" />
                <widget name="info" position="0,0" size="1,1" font="Regular;1" transparent="1" />
                <widget name="text" position="0,0" size="1,1" font="Regular;1" transparent="1" />
                <!--#####red####/-->
                <eLabel backgroundColor="#00ff0000" position="25,700" size="250,6" zPosition="12" />
                <widget name="key_red" position="25,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
                <widget source="key_red" render="Label" position="25,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
                <!--#####green####/-->
                <eLabel backgroundColor="#0000ff00" position="280,700" size="250,6" zPosition="12" />
                <widget name="key_green" position="280,660" size="250,45" zPosition="11" font="Regular;26" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
                <widget source="key_green" render="Label" position="280,660" size="250,45" zPosition="11" font="Regular;26" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
                <!--#####yellow####/-->
                <eLabel backgroundColor="#00ffff00" position="541,700" size="250,6" zPosition="12" />
                <widget name="key_yellow" position="539,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
                <widget source="key_yellow" render="Label" position="539,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
                <!--#####blue####/-->
                <eLabel backgroundColor="#000000ff" position="798,700" size="250,6" zPosition="12" />
                <widget name="key_blue" position="797,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,Button,Label" transparent="1" />
                <widget source="key_blue" render="Label" position="797,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,StaticText" transparent="1" />
            </screen>"""

    def __init__(self, session):
        """Initialize conversion selector screen."""
        Screen.__init__(self, session)
        self.session = session
        self.skinName = "ConversionSelector"
        # self.is_modal = True
        self.setTitle(PLUGIN_TITLE)
        self.menu_options = [
            (_("Enigma2 Bouquets to ➔ Enigma2 Bouquets"), "tv_to_tv", "tv"),
            (_("Enigma2 Bouquets to ➔ M3U"), "tv_to_m3u", "tv"),
            (_("M3U to ➔ Enigma2 Bouquets"), "m3u_to_tv", "m3u"),
            (_("M3U to ➔ JSON"), "m3u_to_json", "m3u"),
            (_("JSON to ➔ Enigma2 Bouquets"), "json_to_tv", "json"),
            (_("JSON to ➔ M3U"), "json_to_m3u", "json"),
            (_("XSPF to ➔ M3U Playlist"), "xspf_to_m3u", "xspf"),
            (_("Remove M3U Bouquets"), "purge_m3u_bouquets", None),
            (_("Plugin Information"), "plugin_info", None)
        ]

        self["list"] = MenuList([(option[0], option[1]) for option in self.menu_options])
        self["Title"] = Label(PLUGIN_TITLE)
        self["info"] = Label('')
        self["text"] = Label('')
        self["status"] = Label(_("We're ready: what do you want to do?"))
        self["actions"] = ActionMap(["ColorActions", "OkCancelActions", "MenuActions"], {
            "red": self.do_final_close,
            "green": self._select_current_item,
            "blue": self._open_epg_importer,
            "menu": self._open_settings,
            "ok": self._select_current_item,
            "yellow": self._purge_m3u_bouquets,
            "cancel": self.do_final_close
        })
        self["key_red"] = StaticText(_("Close"))
        self["key_green"] = StaticText(_("Select"))
        self["key_yellow"] = StaticText(_("Remove Bouquets"))
        self["key_blue"] = StaticText(_("EPGImporter"))

    def _select_current_item(self):
        """Handle selection of current menu item."""
        selection = self["list"].getCurrent()
        if not selection:
            return

        if selection[1] == "purge_m3u_bouquets":
            self._purge_m3u_bouquets()
            return

        if selection[1] == "plugin_info":
            self._show_plugin_info()
            return

        self["status"].setText(_("Press RED to select file"))

        self.session.open(
            UniversalConverter,
            conversion_type=selection[1],
            selected_file=None,
            auto_start=False
        )

    def _open_settings(self):
        """Open plugin settings screen."""
        self.session.open(M3UConverterSettings)

    def _show_plugin_info(self):
        """Show plugin information screen safely"""
        try:
            self.session.open(PluginInfoScreen)
        except Exception as e:
            logger.error(f"Error opening plugin info: {str(e)}")
            self.session.open(
                MessageBox,
                _("Error opening plugin information: %s") % str(e),
                MessageBox.TYPE_ERROR
            )

    def _open_epg_importer(self):
        """Open EPG importer configuration."""
        try:
            from Plugins.Extensions.EPGImport.plugin import EPGImportConfig
            self.session.open(EPGImportConfig)
        except ImportError:
            self.session.open(MessageBox, _("EPGImport plugin not found"), MessageBox.TYPE_ERROR)

    def _purge_m3u_bouquets(self, directory="/etc/enigma2", pattern="_m3ubouquet.tv"):
        """Remove all bouquet files with dynamic EPG cleanup."""
        create_bouquets_backup()
        removed_files = []

        for filename in listdir(directory):
            file_path = join(directory, filename)
            if isfile(file_path) and filename.endswith(pattern):
                try:
                    remove(file_path)
                    removed_files.append(filename)

                    bouquet_name = filename.replace('userbouquet.', '').replace('.tv', '')
                    self._remove_epg_files(bouquet_name)
                    self._remove_epg_bouquet_source(bouquet_name)

                except Exception as e:
                    logger.error("Failed to remove %s: %s", filename, str(e))

        # Clean the main bouquets.tv file
        self._clean_bouquets_file(directory, pattern)

        # Clean EPG sources if empty
        self._clean_epg_sources()

        _reload_services_after_delay()

        if removed_files:
            message = _("Removed {count} bouquet(s):\n{files}").format(
                count=len(removed_files),
                files="\n".join(removed_files)
            )
        else:
            message = _("No M3UConverter bouquets found to remove.")

        self.session.open(MessageBox, message, MessageBox.TYPE_INFO, timeout=6)

    def _clean_bouquets_file(self, directory, pattern):
        """Clean the bouquets.tv file by removing references to deleted bouquets."""
        bouquets_file = join(directory, "bouquets.tv")
        if not exists(bouquets_file):
            return

        try:
            with open(bouquets_file, "r", encoding="utf-8") as f:
                lines = f.readlines()

            # Keep only lines that don't reference the pattern we're removing
            new_lines = []
            for line in lines:
                if pattern in line:
                    continue
                new_lines.append(line)

            with open(bouquets_file, "w", encoding="utf-8") as f:
                f.writelines(new_lines)

        except Exception as e:
            logger.error("Error cleaning bouquets.tv: %s", str(e))

    def _clean_epg_sources(self):
        """Clean the sources.xml file and remove it if empty"""
        epgimport_path = "/etc/epgimport"
        sources_file = join(epgimport_path, "ArchimedeConverter.sources.xml")

        if not fileExists(sources_file):
            return

        try:
            with open(sources_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # Remove sources for non-existing bouquets
            pattern = r'<source type="gen_xmltv"[^>]*channels="(.*?)\.channels\.xml"[^>]*>.*?</source>'

            def should_keep_source(match):
                bouquet_name = match.group(1)
                channels_file = join(epgimport_path, f"{bouquet_name}.channels.xml")
                return fileExists(channels_file)

            # Remove only sources for bouquets that no longer exist
            new_content = sub(pattern, lambda m: m.group(0) if should_keep_source(m) else '', content, flags=DOTALL)

            # Remove empty sourcecat entries
            new_content = sub(r'<sourcecat[^>]*>\s*</sourcecat>', '', new_content)

            # Check if the file has any meaningful content left
            stripped_content = new_content.strip()
            if (not stripped_content or
                stripped_content == '<?xml version="1.0" encoding="utf-8"?><sources></sources>' or
                    'Archimede Converter by Lululla' not in stripped_content):
                # Remove the empty file
                remove(sources_file)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Removed empty EPG sources file")
            else:
                # Write back the cleaned content
                with open(sources_file, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Updated EPG sources file")

        except Exception as e:
            logger.error("Error cleaning EPG sources: %s", str(e))

    def _remove_epg_files(self, bouquet_name):
        """Remove EPG files associated with the bouquet."""
        epgimport_path = "/etc/epgimport"

        # Remove channels.xml file
        channels_file = join(epgimport_path, f"{bouquet_name}.channels.xml")
        if fileExists(channels_file):
            try:
                remove(channels_file)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Removed EPG channels file: %s", channels_file)
            except Exception as e:
                logger.error("Error removing EPG channels file %s: %s", channels_file, str(e))

        # Remove any .tv.epg.imported file (cache file)
        epg_imported_file = join(epgimport_path, f"{bouquet_name}.tv.epg.imported")
        if fileExists(epg_imported_file):
            try:
                remove(epg_imported_file)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Removed EPG cache file: %s", epg_imported_file)
            except Exception as e:
                logger.error("Error removing EPG cache file %s: %s", epg_imported_file, str(e))

    def _remove_epg_bouquet_source(self, bouquet_name):
        """Remove a bouquet from EPG sources."""
        epgimport_path = "/etc/epgimport"
        sources_path = join(epgimport_path, "ArchimedeConverter.sources.xml")

        if not fileExists(sources_path):
            return True

        try:
            with open(sources_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Remove source for this bouquet
            pattern = rf'<source type="gen_xmltv"[^>]*channels="{escape(bouquet_name)}\.channels\.xml"[^>]*>.*?</source>'
            new_content = sub(pattern, '', content, flags=DOTALL)

            # If no more bouquets, remove the entire sourcecat
            sourcecat_pattern = r'<sourcecat sourcecatname="Archimede Converter by Lululla">(.*?)</sourcecat>'
            sourcecat_match = search(sourcecat_pattern, new_content, DOTALL)

            if sourcecat_match:
                # Check if there are any sources left
                sources_content = sourcecat_match.group(1)
                if not search(r'<source type="gen_xmltv"', sources_content):
                    # No sources left, remove entire sourcecat
                    new_content = sub(sourcecat_pattern, '', new_content, flags=DOTALL)

            with open(sources_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Removed EPG source for bouquet: {bouquet_name}")
            return True

        except Exception as e:
            logger.error(f"Error removing EPG source: {str(e)}")
            return False

    def do_final_close(self):
        """Final close procedure - ALWAYS close properly"""
        try:
            _reload_services_after_delay()
            self.close()
        except Exception as e:
            logger.error(f"Error in do_final_close: {str(e)}")
            self.close()


class UniversalConverter(Screen):
    """Main universal converter screen with conversion functionality."""
    if SCREEN_WIDTH > 1280:

        skin = """
        <screen name="UniversalConverter" position="center,center" size="1920,1080" title="Archimede Universal Converter" flags="wfNoBorder">
            <widget source="Title" render="Label" position="64,13" size="1120,52" font="Regular; 32" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1920,1080" zPosition="-2" />
            <widget name="list" position="65,70" size="1122,797" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
            <widget name="status" position="65,920" size="1127,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
            <widget source="progress_source" render="Progress" position="65,875" size="1125,40" backgroundColor="#002d3d5b" transparent="1" foregroundColor="black" />
            <widget source="progress_text" render="Label" position="65,875" size="1124,40" font="Regular;28" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
            <eLabel name="" position="1200,810" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="OK" />
            <eLabel name="" position="1200,865" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="STOP" />
            <eLabel name="" position="1200,920" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="MENU" />
            <widget source="session.CurrentService" render="Label" position="1220,125" size="640,34" font="Regular;26" borderWidth="1" backgroundColor="background" transparent="1" halign="center" foregroundColor="white" zPosition="30" valign="center" noWrap="1">
                <convert type="ServiceName">Name</convert>
            </widget>
            <widget source="session.VideoPicture" render="Pig" position="1220,166" zPosition="20" size="640,360" backgroundColor="transparent" transparent="0" cornerRadius="14" />
            <!-- KEYS -->
            <!--#####red####/-->
            <eLabel backgroundColor="#00ff0000" position="38,1050" size="375,9" zPosition="12" />
            <widget name="key_red" position="38,990" size="375,68" zPosition="11" font="Regular; 34" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
            <widget source="key_red" render="Label" position="38,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
            <!--#####green####/-->
            <eLabel backgroundColor="#0000ff00" position="420,1050" size="375,9" zPosition="12" />
            <widget name="key_green" position="420,990" size="375,68" zPosition="11" font="Regular;32" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
            <widget source="key_green" render="Label" position="420,990" size="375,68" zPosition="11" font="Regular;32" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
            <!--#####yellow####/-->
            <eLabel backgroundColor="#00ffff00" position="812,1050" size="375,9" zPosition="12" />
            <widget name="key_yellow" position="808,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
            <widget source="key_yellow" render="Label" position="808,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
            <!--#####blue####/-->
            <eLabel backgroundColor="#000000ff" position="1197,1050" size="375,9" zPosition="12" />
            <widget name="key_blue" position="1196,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,Button,Label" transparent="1" />
            <widget source="key_blue" render="Label" position="1196,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,StaticText" transparent="1" />
        </screen>"""

    else:
        skin = """
        <screen name="UniversalConverter" position="center,center" size="1280,720" title="Archimede Universal Converter" flags="wfNoBorder">
            <widget source="Title" render="Label" position="25,8" size="1120,52" font="Regular; 24" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1280,720" zPosition="-2" />
            <widget name="list" position="25,60" size="840,518" itemHeight="40" font="Regular;28" scrollbarMode="showNever" />
            <widget name="status" position="24,616" size="1185,42" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
            <widget source="progress_source" render="Progress" position="25,580" size="840,35" backgroundColor="#002d3d5b" transparent="1" foregroundColor="black" />
            <widget source="progress_text" render="Label" position="25,580" size="840,35" font="Regular; 26" backgroundColor="#002d3d5b" transparent="1" foregroundColor="white" />
            <eLabel name="" position="1111,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="OK" />
            <eLabel name="" position="1165,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="STOP" />
            <eLabel name="" position="1220,657" size="52,52" backgroundColor="#003e4b53" halign="center" valign="center" transparent="0" cornerRadius="9" font="Regular; 16" zPosition="1" text="MENU" />
            <widget source="session.CurrentService" render="Label" position="872,54" size="400,34" font="Regular;26" borderWidth="1" backgroundColor="background" transparent="1" halign="center" foregroundColor="white" zPosition="30" valign="center" noWrap="1">
                <convert type="ServiceName">Name</convert>
            </widget>
            <widget source="session.VideoPicture" render="Pig" position="871,92" zPosition="20" size="400,220" backgroundColor="transparent" transparent="0" cornerRadius="14" />
            <!-- KEYS -->
            <!--#####red####/-->
            <eLabel backgroundColor="#00ff0000" position="25,700" size="250,6" zPosition="12" />
            <widget name="key_red" position="25,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
            <widget source="key_red" render="Label" position="25,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
            <!--#####green####/-->
            <eLabel backgroundColor="#0000ff00" position="280,700" size="250,6" zPosition="12" />
            <widget name="key_green" position="280,660" size="250,45" zPosition="11" font="Regular;26" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
            <widget source="key_green" render="Label" position="280,660" size="250,45" zPosition="11" font="Regular;26" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
            <!--#####yellow####/-->
            <eLabel backgroundColor="#00ffff00" position="541,700" size="250,6" zPosition="12" />
            <widget name="key_yellow" position="539,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
            <widget source="key_yellow" render="Label" position="539,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
            <!--#####blue####/-->
            <eLabel backgroundColor="#000000ff" position="798,700" size="250,6" zPosition="12" />
            <widget name="key_blue" position="797,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,Button,Label" transparent="1" />
            <widget source="key_blue" render="Label" position="797,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,StaticText" transparent="1" />
        </screen>"""

    def __init__(self, session, conversion_type=None, selected_file=None, auto_start=False):
        Screen.__init__(self, session)
        self.session = session
        self.conversion_type = conversion_type
        self.selected_file = selected_file
        self.auto_start = auto_start
        title_mapping = {
            "tv_to_tv": _("Enigma2 Bouquet to Enigma2 Bouquet Conversion"),
            "m3u_to_tv": _("M3U to Enigma2 Bouquet Conversion"),
            "tv_to_m3u": _("Enigma2 Bouquet to M3U Conversion"),
            "json_to_tv": _("JSON to Enigma2 Bouquet Conversion"),
            "json_to_m3u": _("JSON to M3U Conversion"),
            "xspf_to_m3u": _("XSPF to M3U Playlist Conversion"),
            "m3u_to_json": _("M3U to JSON Conversion")
        }
        title_text = title_mapping.get(conversion_type, PLUGIN_TITLE)
        self.setTitle(title_text)
        self.m3u_channels_list = []
        # self.bouquet_list = []
        self.aspect_manager = AspectManager()
        self.core_converter = core_converter
        self.progress = None
        self.is_converting = False
        self.cancel_conversion = False
        self.epg_mapper = None
        self.last_conversion_stats = None
        self.last_cache_stats = None
        base_path = config.plugins.m3uconverter.lastdir.value
        self.full_path = base_path
        self["list"] = MenuList([])
        self["Title"] = Label(title_text)
        self["status"] = Label(_("Ready"))
        self["key_red"] = StaticText(_("Open File"))
        self["key_green"] = StaticText("")
        self["key_yellow"] = StaticText("")
        self["key_blue"] = StaticText(_("Tools"))
        self.progress_source = Progress()
        self["progress_source"] = self.progress_source
        self["progress_text"] = StaticText("")
        self["progress_source"].setValue(0)
        self.initial_service = self.session.nav.getCurrentlyPlayingServiceReference()
        self["actions"] = ActionMap(["ColorActions", "OkCancelActions", "MediaPlayerActions", "MenuActions"], {
            "red": self._open_file_browser,
            "green": self._start_conversion_process,
            "yellow": self._open_manual_match_editor,
            "blue": self._handle_blue_button_action,
            "menu": self._open_settings,
            "ok": self._handle_ok_button,
            "cancel": self._close_screen,
            "stop": self._stop_media_player
        }, -1)
        self["status"].setText(_("Ready: Select the file from the %s you configured in settings.") % self.full_path)

        if self.conversion_type == "tv_to_m3u":
            self._initialize_tv_converter()

        if self.selected_file:
            self["status"].setText(_("File loaded: {}").format(basename(self.selected_file)))
            self.file_loaded = True
        else:
            self["status"].setText(_("Press RED to select file"))
            self.file_loaded = False

        self.epg_mapper = self._initialize_epg_mapper()

    def _initialize_epg_mapper(self):
        """Initialize EPG mapper"""

        # CHECK IF ALREADY EXISTS to avoid double initialization
        if hasattr(self, 'epg_mapper') and self.epg_mapper is not None:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("✅ EPG_MAPPER already initialized, reusing")
            return self.epg_mapper

        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("🔄 Creating EPGServiceMapper...")
            self.epg_mapper = EPGServiceMapper(prefer_satellite=True)

            # LOAD ALL DATABASES SEQUENTIALLY - ONLY ONCE
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("📥 Loading all databases...")

            # 1. First local databases (essential)
            self.epg_mapper._parse_lamedb()
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"✅ Lamedb loaded: {len(self.epg_mapper.mapping.dvb)} channels")

            self.epg_mapper._parse_existing_bouquets()
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("✅ Existing bouquets loaded")

            # 2. FORCE Rytec loading with debug
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("🔍 LOADING RYTEC DATABASE...")
            rytec_paths = [
                "/etc/epgimport/rytec.channels.xml",
                "/usr/lib/enigma2/python/Plugins/Extensions/EPGImport/rytec.channels.xml",
            ]

            for rytec_path in rytec_paths:
                if fileExists(rytec_path):
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"📁 Rytec file found: {rytec_path}")
                    self.epg_mapper._parse_rytec_channels(rytec_path)

                    # check 'basic' instead of 'extended'
                    rytec_count = len(self.epg_mapper.mapping.rytec['basic'])
                    if rytec_count > 0:
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info(f"✅ Rytec database loaded: {rytec_count} channels")
                        break
                    else:
                        logger.error(f"❌ Rytec file exists but 0 channels loaded from: {rytec_path}")
                else:
                    logger.warning(f"📁 File not found: {rytec_path}")
            # 3. Channel mapping and optimizations
            self.epg_mapper._load_channel_mapping()
            if config.plugins.m3uconverter.ignore_dvbt.value:
                self.epg_mapper._clear_dvbt_services()

            self.epg_mapper.optimize_matching()

            # clean csv < 20mb
            self.epg_mapper._cleanup_smart()

            # FINAL CHECK
            final_rytec = len(self.epg_mapper.mapping.rytec.get('basic', {}))
            final_dvb = len(self.epg_mapper.mapping.dvb)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"🎯 FINAL DATABASE STATUS: Rytec={final_rytec}, DVB={final_dvb}")

            return self.epg_mapper

        except Exception as e:
            logger.error(f"❌ EPG Mapper initialization failed: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Still create a fallback instance
            self.epg_mapper = EPGServiceMapper(prefer_satellite=True)
            return self.epg_mapper

    def _open_file_browser(self):
        """Open file browser for file selection."""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"Opening file browser for {self.conversion_type}")

        try:
            # Determina il percorso iniziale in base al tipo di conversione
            if self.conversion_type in ["tv_to_tv", "enigmatoenigma"]:
                path = "/etc/enigma2"
            elif self.conversion_type == "tv_to_m3u":
                path = "/etc/enigma2"
            else:
                path = config.plugins.m3uconverter.lastdir.value

            pattern = r"(?i)^.*\.(tv|m3u|m3u8|json|xspf)$"

            if not path or not isdir(path):
                path = "/media/hdd" if isdir("/media/hdd") else "/tmp"

            self.session.openWithCallback(
                self._handle_file_selection,
                M3UFileBrowser,
                startdir=path,
                matchingPattern=pattern,
                conversion_type=self.conversion_type
            )

        except Exception as e:
            logger.error(f"Error opening file browser: {str(e)}")
            self.session.open(
                MessageBox,
                _("Error opening file browser:\n%s") % str(e),
                MessageBox.TYPE_ERROR,
                timeout=6
            )

    def _start_conversion_process(self):
        """Start the conversion process."""
        if self.is_converting:
            return

        if not hasattr(self, 'selected_file') or not self.selected_file:
            self.session.open(MessageBox, _("No file selected for conversion"), MessageBox.TYPE_WARNING)
            return

        # Update UI based on conversion type
        conversion_labels = {
            "tv_to_tv": _("Convert TV to TV"),
            "m3u_to_tv": _("Convert M3U to TV"),
            "tv_to_m3u": _("Convert TV to M3U"),
            "json_to_tv": _("Convert JSON to TV"),
            "json_to_m3u": _("Convert JSON to M3U"),
            "xspf_to_m3u": _("Convert XSPF to M3U"),
            "m3u_to_json": _("Convert M3U to JSON")
        }

        green_label = conversion_labels.get(self.conversion_type, _("Convert"))
        self["key_green"].setText(green_label)
        self["status"].setText(_("Conversion in progress..."))

        # Handle different conversion types
        if self.conversion_type == "m3u_to_tv":
            self._convert_m3u_to_tv()
        elif self.conversion_type == "m3u_to_json":
            self._convert_m3u_to_json()
        elif self.conversion_type == "tv_to_tv":
            self._convert_tv_to_tv()
        elif self.conversion_type == "tv_to_m3u":
            self._convert_tv_to_m3u()
        elif self.conversion_type == "json_to_tv":
            self._convert_json_to_tv()
        elif self.conversion_type == "json_to_m3u":
            self._convert_json_to_m3u()
        elif self.conversion_type == "xspf_to_m3u":
            self._convert_xspf_to_m3u()
        else:
            self.session.open(MessageBox, _("Unsupported conversion type"), MessageBox.TYPE_ERROR)

    def _open_manual_match_editor(self):
        """Open manual editor from main screen - RETURN TO UniversalConverter"""

        if not hasattr(self, 'm3u_channels_list') or not self.m3u_channels_list:
            self.session.open(MessageBox, _("No conversion data available."), MessageBox.TYPE_WARNING)
            return

        bouquet_name = ""
        if hasattr(self, 'selected_file') and self.selected_file:
            bouquet_name = basename(self.selected_file).split('.')[0]

        def editor_closed(result=None):
            """Callback when the manual editor closes - return to UniversalConverter"""
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Manual editor closed, returning to UniversalConverter")
            try:
                # Just show confirmation and log it
                self["status"].setText(_("Manual editing completed - ready for conversion"))
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("✅ Returned to UniversalConverter successfully")
            except Exception as e:
                logger.error(f"Error updating after editor close: {str(e)}")

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Opening ManualMatchEditor from main for: {bouquet_name}")

        # Convert tuple data to dictionary format if needed
        processed_data = []
        for channel in self.m3u_channels_list:
            if isinstance(channel, tuple) and len(channel) >= 2:
                # Convert (name, url) tuple to dictionary
                processed_data.append({
                    'name': channel[0],
                    'url': channel[1],
                    'original_name': channel[0],
                    'match_type': 'tv_bouquet'
                })
            elif isinstance(channel, dict):
                processed_data.append(channel)
            else:
                logger.warning(f"Skipping invalid channel format: {type(channel)}")

        self.session.openWithCallback(
            editor_closed,
            ManualMatchEditor,
            processed_data,  # Use the processed data instead
            self.epg_mapper,
            bouquet_name
        )

    def _handle_blue_button_action(self):
        """Dynamic handling of the blue button based on current state."""
        if self.is_converting:
            self._cancel_conversion_process()
        else:
            self._show_enhanced_tools_menu()

    def _open_settings(self):
        """Open plugin settings screen."""
        self.session.open(M3UConverterSettings)

    def _handle_ok_button(self):
        """Handle OK button press for channel playback."""
        index = self["list"].getSelectedIndex()
        if index < 0 or index >= len(self.m3u_channels_list):
            self["status"].setText(_("No file selected"))
            return

        item = self.m3u_channels_list[index]

        if self.conversion_type == "tv_to_m3u":
            # If loaded from .tv (tuple)
            if isinstance(item, tuple) and len(item) == 2:
                name, url = item
                url = unquote(url.strip())
            else:
                self["status"].setText(_("Invalid .tv item format"))
                return
        else:
            # Default: assume it's from .m3u (dict)
            if isinstance(item, dict):
                name = item.get("name", "")
                url = item.get("url", "")
                url = unquote(url.strip())
            else:
                self["status"].setText(_("Invalid .m3u item format"))
                return

        self["status"].setText(_("Playing: %s") % name)
        self.start_player(name, url)

    def _close_screen(self, result=None):
        try:
            if hasattr(self, 'initial_service') and self.initial_service:
                self.session.nav.playService(self.initial_service)

            if hasattr(self, 'aspect_manager'):
                self.aspect_manager.restore_aspect()

            self.close()
        except Exception as e:
            logger.error(f"Error during close: {str(e)}")
            self.close()

    def _stop_media_player(self):
        """Stop media player and restore original service."""
        self.session.nav.stopService()
        if hasattr(self, 'initial_service') and self.initial_service:
            self.session.nav.playService(self.initial_service)
        self["status"].setText(_("Ready"))

    def reset_conversion_buttons(self):
        """Reset all conversion buttons to default state"""
        return self._reset_conversion_ui()

    def _start_conversion_after_display(self):
        """Automatically start conversion after screen is displayed."""
        try:
            self.onShown.remove(self._start_conversion_after_display)
            if self.auto_start and self.selected_file:
                self.start_timer = eTimer()
                self.start_timer.callback.append(self._delayed_conversion_start)
                self.start_timer.start(2000)  # 2 second delay
        except Exception:
            pass

    def _delayed_conversion_start(self):
        """Start conversion with a slight delay."""
        try:
            self.start_timer.stop()
            self._start_conversion_process()
        except Exception as e:
            logger.error(f"Error in delayed conversion start: {str(e)}")

    def _initialize_tv_converter(self):
        """Initialize TV converter specific settings."""
        self._update_tv_path_settings()

    def _update_tv_path_settings(self):
        """Update TV path settings and check permissions."""
        try:
            if not exists("/etc/enigma2"):
                raise OSError("Bouquets path not found")

            if not access("/etc/enigma2", W_OK):
                logger.warning("Read-only bouquets path /etc/enigma2")

        except Exception as e:
            logger.error(f"TV path error: {str(e)}")
            self.session.open(MessageBox, str(e), MessageBox.TYPE_ERROR, timeout=6)

    def _show_enhanced_tools_menu(self):
        """Show enhanced tools menu with various utilities."""
        menu_items = [
            (_("📋 Plugin Info"), "info"),
            (_("📊 EPG Cache Statistics"), "cache_stats"),
            (_("💾 Create Backup"), "backup"),
            (_("🔄 Reload EPG Database"), "reload_epg"),
            (_("🔄 Reload Services"), "reload"),
            (_("🧹 Clear EPG Cache"), "clear_cache"),
            (_("🗑️ Clear Log File"), "clear_log"),

            (_("🗃️ === Manual Database Management ==="), "header"),
            (_("✏️ Open Database Editor"), "open_db_editor"),
            (_("👁️ View Manual Database"), "view_manual_db"),
            (_("📝 Manual Match Editing"), "match_edit"),
            (_("📤 Export Manual Database"), "export_manual_db"),
            (_("📥 Import Manual Database"), "import_manual_db"),
            (_("🧹 Clean Manual Database"), "clean_manual_db"),
        ]

        def tool_selection_handler(choice):
            """Handle tool selection from menu."""
            if not choice:
                return

            if choice[1] not in ["header", "separator"]:
                action = choice[1]
                if action == "open_db_editor":
                    self._open_manual_database_editor()
                elif action == "cache_stats":
                    self._show_cache_statistics()
                elif action == "reload_epg":
                    self._reload_epg_database()
                elif action == "clear_cache":
                    self._clear_epg_cache()
                elif action == "backup":
                    self._create_manual_backup()
                elif action == "reload":
                    self._reload_services()
                elif action == "info":
                    self.session.openWithCallback(
                        lambda result=None: self._show_enhanced_tools_menu(),  # TORNA AL MENU TOOLS
                        PluginInfoScreen
                    )
                elif action == "match_edit":
                    self._open_manual_match_editor_from_tools()
                elif action == "view_manual_db":
                    self._view_manual_database()
                elif action == "clean_manual_db":
                    self._clean_manual_database()
                elif action == "export_manual_db":
                    self._export_manual_database()
                elif action == "import_manual_db":
                    self._import_manual_database()
                elif action == "clear_log":
                    self._clear_log_file()

        self.session.openWithCallback(
            tool_selection_handler,
            ChoiceBox,
            title=_("Advanced Tools Menu"),
            list=menu_items
        )

    def _create_tools_callback(self):
        """Create a callback that does NOT reopen the Tools menu"""
        def callback(result=None):
            pass
        return callback

    def _open_manual_database_editor(self):
        """Open the manual database editor from Tools - RETURN TO Tools"""

        def editor_closed(result=None):
            """Return to Tools menu, NOT to UniversalConverter"""
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Database editor closed, RE-opening tools menu")
            # Reopen the Tools menu
            self._show_enhanced_tools_menu()

        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.session.openWithCallback(
                editor_closed,
                ManualDatabaseEditor,
                self.epg_mapper
            )
        else:
            self.session.open(
                MessageBox,
                _("EPG mapper not available"),
                MessageBox.TYPE_ERROR
            )

    def _show_cache_statistics(self):
        """Display EPG cache statistics"""
        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("SHOW_CACHE_STATISTICS")
            # FIRST look for preserved statistics from last conversion
            if hasattr(self, 'last_cache_stats') and self.last_cache_stats:
                stats = self.last_cache_stats
                source = "Last conversion"
            elif hasattr(self, 'epg_mapper') and self.epg_mapper:
                stats = self.epg_mapper._get_cache_statistics()
                source = "Current cache"
            else:
                self.session.openWithCallback(
                    lambda result=None: self._show_enhanced_tools_menu(),
                    MessageBox,
                    _("No statistics available"),
                    MessageBox.TYPE_INFO,
                    timeout=6
                )
                return

            # Check if we have meaningful statistics
            total_matches = stats.get('total_matches', 0)
            if total_matches == 0:
                self.session.openWithCallback(
                    lambda result=None: self._show_enhanced_tools_menu(),
                    MessageBox,
                    _("No conversion statistics available.\nRun a conversion first to see statistics."),
                    MessageBox.TYPE_INFO,
                    timeout=6
                )
                return

            if total_matches == 0:
                message_lines = [
                    _("📊 EPG STATISTICS - {}").format(source),
                    "",
                    _("No conversion statistics available."),
                    _("Run a conversion first to see statistics.")
                ]
                self.session.open(MessageBox, "\n".join(message_lines), MessageBox.TYPE_INFO)
                return

            # Calculate meaningful statistics
            rytec_matches = stats.get('rytec_matches', 0)
            dvb_matches = stats.get('dvb_matches', 0)
            dvbt_matches = stats.get('dvbt_matches', 0)
            fallback_matches = stats.get('fallback_matches', 0)
            manual_db_matches = stats.get('manual_db_matches', 0)

            # Calculate percentages
            rytec_percent = (rytec_matches / total_matches) * 100 if total_matches > 0 else 0
            dvb_percent = (dvb_matches / total_matches) * 100 if total_matches > 0 else 0
            dvbt_percent = (dvbt_matches / total_matches) * 100 if total_matches > 0 else 0
            fallback_percent = (fallback_matches / total_matches) * 100 if total_matches > 0 else 0
            manual_percent = (manual_db_matches / total_matches) * 100 if total_matches > 0 else 0
            epg_coverage = 100 - fallback_percent

            message_lines = [
                _("📊 EPG STATISTICS - {}").format(source),
                "",
                _("🎯 TOTAL CHANNELS: {}").format(total_matches),
                _("📈 EPG COVERAGE: {:.1f}%").format(epg_coverage),
                "",
                _("🔧 MATCH BREAKDOWN:"),
                _("• 🛰️ Rytec: {} ({:.1f}%)").format(rytec_matches, rytec_percent),
                _("• 📡 DVB-S: {} ({:.1f}%)").format(dvb_matches, dvb_percent),
                _("• 📺 DVB-T: {} ({:.1f}%)").format(dvbt_matches, dvbt_percent),
                _("• 💾 Manual DB: {} ({:.1f}%)").format(manual_db_matches, manual_percent),
                _("• 🔌 IPTV Fallback: {} ({:.1f}%)").format(fallback_matches, fallback_percent),
            ]

            # Add cache statistics if available
            cache_hits = stats.get('match_hits', 0)
            cache_misses = stats.get('match_misses', 0)
            total_requests = cache_hits + cache_misses

            if total_requests > 0:
                cache_hit_rate = stats.get('match_hit_rate', '0%')
                if isinstance(cache_hit_rate, str) and '%' in cache_hit_rate:
                    cache_hit_rate_value = cache_hit_rate.replace('%', '')
                else:
                    cache_hit_rate_value = (cache_hits / total_requests) * 100 if total_requests > 0 else 0

                message_lines.extend([
                    "",
                    _("💾 CACHE PERFORMANCE:"),
                    _("• Hit: {} ({:.1f}%)").format(cache_hits, float(cache_hit_rate_value)),
                    _("• Miss: {}").format(cache_misses),
                    _("• Size: {} entries").format(stats.get('match_cache_size', 0))
                ])

            # Database info
            message_lines.extend([
                "",
                _("🗄️ DATABASE INFO:"),
                _("• Rytec: {} channels").format(stats.get('rytec_channels', 0)),
                _("• DVB: {} channels").format(stats.get('loaded_dvb_channels', 0)),
                _("• Mode: {}").format(stats.get('database_mode', 'N/A'))
            ])

            def stats_closed(result=None):
                self._show_enhanced_tools_menu()

            self.session.openWithCallback(
                stats_closed,
                MessageBox,
                "\n".join(message_lines),
                MessageBox.TYPE_INFO,
                timeout=15
            )

        except Exception as e:
            logger.error(f"Error showing cache statistics: {e}")
            self._show_enhanced_tools_menu()

    def _reload_epg_database(self):
        """Reload EPG database."""
        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Starting EPG mapper reinitialization...")
            self.epg_mapper = EPGServiceMapper(prefer_satellite=True)
            self.epg_mapper._refresh_config()

            if self.epg_mapper.initialize():
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("EPG mapper reinitialized successfully")
                self.session.openWithCallback(
                    lambda result=None: self._show_enhanced_tools_menu(),
                    MessageBox,
                    _("EPG database reloaded successfully!"),
                    MessageBox.TYPE_INFO,
                    timeout=6
                )
            else:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("EPG mapper initialization failed")
                self.session.openWithCallback(
                    lambda result=None: self._show_enhanced_tools_menu(),
                    MessageBox,
                    _("Failed to initialize EPG mapper - check logs"),
                    MessageBox.TYPE_ERROR
                )

        except Exception as e:
            logger.error(f"Error reloading EPG database: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            self.session.openWithCallback(
                lambda result=None: self._show_enhanced_tools_menu(),
                MessageBox,
                _("Error reloading EPG database: {}").format(str(e)),
                MessageBox.TYPE_ERROR
            )

    def _clear_epg_cache(self):
        """Clear EPG cache."""
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            try:
                match_cache_size = len(self.epg_mapper._match_cache)
                self.epg_mapper.reset_caches(clear_match_cache=True)

                self.session.openWithCallback(
                    lambda x: self._show_enhanced_tools_menu(),
                    MessageBox,
                    _("EPG cache cleared! {} entries removed").format(match_cache_size),
                    MessageBox.TYPE_INFO,
                    timeout=6
                )

            except Exception as e:
                logger.error(f"Error clearing EPG cache: {e}")
                self.session.openWithCallback(
                    lambda x: self._show_enhanced_tools_menu(),
                    MessageBox,
                    _("Error clearing EPG cache"),
                    MessageBox.TYPE_ERROR
                )
        else:
            self.session.openWithCallback(
                lambda x: self._show_enhanced_tools_menu(),
                MessageBox,
                _("EPG mapper not initialized"),
                MessageBox.TYPE_WARNING
            )

    def _create_manual_backup(self):
        """Create manual backup of bouquets."""
        try:
            self.core_converter._create_backup()
            self.session.openWithCallback(
                lambda result=None: self._show_enhanced_tools_menu(),
                MessageBox,
                _("Backup created successfully!"),
                MessageBox.TYPE_INFO,
                timeout=6
            )
        except Exception as e:
            self.session.openWithCallback(
                lambda result=None: self._show_enhanced_tools_menu(),
                MessageBox,
                _("Backup failed: {}").format(str(e)),
                MessageBox.TYPE_ERROR,
                timeout=6
            )

    def _reload_services(self):
        """Reload Enigma2 services."""
        try:
            self["status"].setText(_("Reloading services..."))

            # Add a short delay before reload to ensure all files are written
            def delayed_reload():
                _reload_services_after_delay()  # 4-second delay
                self["status"].setText(_("Service reload initiated"))

                # Show confirmation message
                self.session.openWithCallback(
                    lambda result=None: self._show_enhanced_tools_menu(),
                    MessageBox,
                    _("Service reload command sent successfully.\nBouquets should appear shortly."),
                    MessageBox.TYPE_INFO,
                    timeout=5
                )

            # Run reload in a separate thread
            thread = threading.Thread(target=delayed_reload, daemon=True)
            thread.start()

        except Exception as e:
            logger.error(f"Error reloading services: {e}")
            self.session.openWithCallback(
                lambda result=None: self._show_enhanced_tools_menu(),
                MessageBox,
                _("Error initiating service reload"),
                MessageBox.TYPE_ERROR,
                timeout=3
            )

    def _open_manual_match_editor_from_tools(self):
        """Open manual editor from Tools menu - RETURN TO Tools"""

        if not hasattr(self, 'm3u_channels_list') or not self.m3u_channels_list:

            def error_callback(result=None):
                self._show_enhanced_tools_menu()

            self.session.openWithCallback(
                error_callback,
                MessageBox,
                _("No conversion data available."),
                MessageBox.TYPE_WARNING
            )
            return

        bouquet_name = ""
        if hasattr(self, 'selected_file') and self.selected_file:
            bouquet_name = basename(self.selected_file).split('.')[0]

        def editor_closed(result=None):
            """Callback when editor closes - RE-open the Tools menu"""
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Editor closed, RE-opening tools menu")
            self._show_enhanced_tools_menu()

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Opening ManualMatchEditor from tools for: {bouquet_name}")

        self.session.openWithCallback(
            editor_closed,
            ManualMatchEditor,
            self.m3u_channels_list,
            self.epg_mapper,
            bouquet_name
        )

    def _view_manual_database(self):
        """Show all manual corrections with option to EDIT or DELETE"""
        try:
            if not hasattr(self, 'epg_mapper') or not hasattr(self.epg_mapper, 'manual_db'):
                self.session.openWithCallback(
                    self._create_tools_callback(),
                    MessageBox,
                    _("Manual database not available"),
                    MessageBox.TYPE_WARNING
                )
                return

            manual_db = self.epg_mapper.manual_db
            data = manual_db.load_database()
            mappings = data.get('mappings', [])

            if not mappings:
                self.session.openWithCallback(
                    self._create_tools_callback(),
                    MessageBox,
                    _("No manual corrections found"),
                    MessageBox.TYPE_INFO,
                    timeout=6
                )
                return

            # Create list for display with EDIT option
            items = []
            for i, mapping in enumerate(mappings):
                channel_name = mapping.get('channel_name', 'Unknown')
                match_type = mapping.get('match_type', '')
                usage_count = mapping.get('usage_count', 0)

                display_text = f"{i + 1}. {channel_name} [{match_type}] (Used: {usage_count}x)"
                items.append((display_text, mapping))

            def choice_callback(selected_item):
                if selected_item is None:
                    # User pressed EXIT - return to tools
                    self._show_enhanced_tools_menu()
                    return

                # User selected an item - show options (EDIT or DELETE)
                mapping = selected_item[1]
                channel_name = mapping.get('channel_name', 'Unknown')

                options = [
                    (_("✏️ Edit this mapping"), "edit"),
                    (_("🗑️ Delete this mapping"), "delete"),
                    (_("🔙 Back"), "back")
                ]

                def action_callback(action_choice):
                    if not action_choice:
                        self._view_manual_database()
                        return

                    if action_choice[1] == "edit":
                        # Open ManualMatchEditor for this mapping
                        self._open_mapping_editor(mapping)
                    elif action_choice[1] == "delete":
                        # Ask for deletion confirmation
                        self._delete_manual_mapping(mapping)
                    elif action_choice[1] == "back":
                        # Return to the list
                        self._view_manual_database()

                self.session.openWithCallback(
                    action_callback,
                    ChoiceBox,
                    title=_("Action for: {}").format(channel_name),
                    list=options
                )

            self.session.openWithCallback(
                choice_callback,
                ChoiceBox,
                title=_("Manual Corrections ({} total)").format(len(mappings)),
                list=items
            )

        except Exception as e:
            logger.error(f"Error viewing manual database: {str(e)}")
            self._show_enhanced_tools_menu()

    def _clean_manual_database(self):
        """Clean/clear the entire manual database - STAY IN TOOLS"""
        try:
            message = _(
                "This will delete ALL manual corrections.\n\n"
                "Total corrections: {}\n"
                "This action cannot be undone.\n\n"
                "Are you sure?"
            )
            manual_db = self.epg_mapper.manual_db
            data = manual_db.load_database()
            mapping_count = len(data.get('mappings', []))

            def confirm_callback(result):
                if result is not None and result:
                    success = self._perform_clean_database()
                    if success:
                        self.session.openWithCallback(
                            lambda x: self._show_enhanced_tools_menu(),
                            MessageBox,
                            _("Manual database cleared"),
                            MessageBox.TYPE_INFO,
                            timeout=6
                        )
                    else:
                        self.session.openWithCallback(
                            lambda x: self._show_enhanced_tools_menu(),
                            MessageBox,
                            _("Error clearing database"),
                            MessageBox.TYPE_ERROR
                        )
                else:
                    self._show_enhanced_tools_menu()

            self.session.openWithCallback(
                confirm_callback,
                MessageBox,
                message.format(mapping_count),
                MessageBox.TYPE_YESNO
            )

        except Exception as e:
            logger.error(f"Error cleaning manual database: {str(e)}")
            self._show_enhanced_tools_menu()

    def _export_manual_database(self):
        """Export manual database to file"""
        try:
            manual_db = self.epg_mapper.manual_db
            data = manual_db.load_database()
            mapping_count = len(data.get('mappings', []))

            if mapping_count == 0:
                self.session.openWithCallback(
                    lambda result=None: self._show_enhanced_tools_menu(),
                    MessageBox,
                    _("No manual corrections to export"),
                    MessageBox.TYPE_INFO,
                    timeout=6
                )
                return

            export_dir = join(PLUGIN_PATH, "database")

            # Create the directory if it doesn't exist
            if not exists(export_dir):
                makedirs(export_dir, exist_ok=True)

            # Clean ONLY exports (not backups)
            export_pattern = join(export_dir, "manual_mappings_export_*.json")
            old_exports = sorted(glob.glob(export_pattern))

            # Remove old exports if there are more than 2
            while len(old_exports) >= 3:
                oldest_export = old_exports[0]
                try:
                    remove(oldest_export)
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"🧹 Removed old export: {basename(oldest_export)}")
                    old_exports.pop(0)
                except Exception as e:
                    logger.error(f"❌ Error removing export {oldest_export}: {str(e)}")
                    break

            timestamp = strftime("%Y%m%d_%H%M%S")
            export_filename = f"manual_mappings_export_{timestamp}.json"
            export_path = join(export_dir, export_filename)

            with open(export_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            message = _(
                "Manual database exported successfully!\n\n"
                "File: {}\n"
                "Location: {}\n"
                "Corrections: {}\n"
                "Size: {} bytes"
            ).format(
                export_filename,
                export_dir,
                mapping_count,
                getsize(export_path) if exists(export_path) else 0
            )

            self.session.openWithCallback(
                lambda result=None: self._show_enhanced_tools_menu(),
                MessageBox,
                message,
                MessageBox.TYPE_INFO
            )

        except Exception as e:
            logger.error(f"Error exporting manual database: {str(e)}")
            self.session.openWithCallback(
                lambda result=None: self._show_enhanced_tools_menu(),
                MessageBox,
                _("Error exporting database"),
                MessageBox.TYPE_ERROR
            )

    def _import_manual_database(self):
        """Import manual database from backup or external file"""
        try:
            # Define import locations
            import_locations = [
                (join(PLUGIN_PATH, "database"), _("Database Directory")),
                ("/tmp", _("Temporary Files")),
            ]

            def location_selected(choice):
                if not choice:
                    self._show_enhanced_tools_menu()  # Return to Tools if you cancel
                    return

                base_import_path = choice[1]

                # Search Files - Separate Backups from Exports
                file_patterns = [
                    "manual_mappings_export_*.json",           # Export files
                    "manual_mappings.json",              # Main database
                    "manual_mappings.json.backup_*",     # Backup files
                ]

                found_files = []
                for pattern in file_patterns:
                    full_pattern = join(base_import_path, pattern)
                    found_files.extend(glob.glob(full_pattern))

                if not found_files:
                    # Return to location selection, not Tools
                    def no_files_callback(result=None):
                        # Reopen location selection
                        self._import_manual_database()

                    self.session.openWithCallback(
                        no_files_callback,
                        MessageBox,
                        _("No database files found in:\n{}").format(base_import_path),
                        MessageBox.TYPE_INFO,
                        timeout=6
                    )
                    return

                # Remove duplicates and sort by date
                found_files = list(set(found_files))
                found_files.sort(key=lambda x: getmtime(x) if exists(x) else 0, reverse=True)

                # Create file list for selection
                file_items = []
                for file_path in found_files:
                    filename = basename(file_path)
                    file_time = strftime("%Y-%m-%d %H:%M", time.localtime(getmtime(file_path)))

                    # Determine file type
                    file_type = "DB"
                    if "export" in filename:
                        file_type = "EXPORT"
                    elif "backup" in filename:
                        file_type = "BACKUP"

                    # Mapping count info
                    mapping_count = "?"
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            mapping_count = str(len(data.get('mappings', [])))
                    except:
                        pass

                    display_text = f"{filename} [{file_type}] ({mapping_count} maps, {file_time})"
                    file_items.append((display_text, file_path))

                file_items.append((_("🔙 Back to locations"), "back"))

                def file_selected(file_choice):
                    if not file_choice or file_choice[1] == "back":
                        # Return to location selection
                        self._import_manual_database()
                        return

                    file_path = file_choice[1]
                    self._perform_database_import(file_path)

                self.session.openWithCallback(
                    file_selected,
                    ChoiceBox,
                    title=_("Select file to import from {}").format(basename(base_import_path)),
                    list=file_items
                )

            location_items = [(desc, path) for path, desc in import_locations]
            location_items.append((_("🔙 Back to Tools"), "back"))

            def location_callback(choice):
                if not choice or choice[1] == "back":
                    self._show_enhanced_tools_menu()  # Return to Tools only if you press Back
                    return
                location_selected(choice)

            self.session.openWithCallback(
                location_callback,
                ChoiceBox,
                title=_("Select import location"),
                list=location_items
            )

        except Exception as e:
            logger.error(f"Error starting database import: {str(e)}")
            # If you get an error, go back to Tools
            self.session.openWithCallback(
                lambda result=None: self._show_enhanced_tools_menu(),
                MessageBox,
                _("Error starting import: {}").format(str(e)),
                MessageBox.TYPE_ERROR
            )

    def _clear_log_file(self):
        """Manually clear the log file"""
        try:
            if hasattr(self, 'epg_mapper') and self.epg_mapper:
                self.epg_mapper._cleanup_log_file()
                self.session.openWithCallback(
                    lambda x: self._show_enhanced_tools_menu(),
                    MessageBox,
                    _("Log file cleared successfully"),
                    MessageBox.TYPE_INFO,
                    timeout=6
                )
        except Exception as e:
            logger.error(f"Error clearing log file: {str(e)}")

    def _delayed_open_conversion_selector(self):
        """Apri ConversionSelector dopo un piccolo delay"""
        try:
            self.session.open(ConversionSelector)
        except Exception as e:
            logger.error(f"Error opening ConversionSelector: {str(e)}")

        def _delayed_return_to_main_from_editor(self):
            """Delayed return to main screen from editor"""
            try:
                if hasattr(self, 'return_timer'):
                    self.return_timer.stop()

                self.close()

                self.final_return_timer = eTimer()
                self.final_return_timer.callback.append(self._open_conversion_selector_from_editor)
                self.final_return_timer.start(50, True)

            except Exception as e:
                logger.error(f"Error in delayed return from editor: {str(e)}")

    def _open_conversion_selector_from_editor(self):
        """Safely open ConversionSelector from editor"""
        try:
            if hasattr(self, 'final_return_timer'):
                self.final_return_timer.stop()
            self.session.open(ConversionSelector)
        except Exception as e:
            logger.error(f"Error opening ConversionSelector from editor: {str(e)}")

    def _open_mapping_editor(self, mapping):
        """Open ManualMatchEditor for a specific mapping"""
        try:
            # Create channel data for the editor
            channel_data = [{
                'name': mapping.get('channel_name', 'Unknown'),
                'original_name': mapping.get('channel_name', 'Unknown'),
                'sref': mapping.get('assigned_sref', ''),
                'match_type': mapping.get('match_type', 'manual'),
                'tvg_id': mapping.get('tvg_id', ''),
                'url': mapping.get('url', ''),
                'group': 'Manual Database',
                'original_sref': mapping.get('assigned_sref', '')
            }]

            def editor_closed(result=None):
                """Callback when editor closes - returns to database view"""
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("ManualMatchEditor closed")
                self._view_manual_database()

            self.session.openWithCallback(
                editor_closed,
                ManualMatchEditor,
                channel_data,
                self.epg_mapper,
                "manual_database_edit",
                parent_callback=editor_closed
            )

        except Exception as e:
            logger.error(f"Error opening mapping editor: {str(e)}")

    def _delete_manual_mapping(self, mapping):
        """Delete a specific manual mapping and return to list"""
        try:
            channel_name = mapping.get('channel_name', 'Unknown')
            message = _("Delete manual correction for:\n{}\n\nThis action cannot be undone.").format(channel_name)

            def confirm_callback(result):
                if result:
                    success = self._perform_delete_mapping(mapping)
                    if success:
                        # After deletion, show the list again
                        self._view_manual_database()
                    else:
                        self.session.open(
                            MessageBox,
                            _("Error deleting correction"),
                            MessageBox.TYPE_ERROR
                        )
                else:
                    # User cancelled deletion, return to list
                    self._view_manual_database()

            self.session.openWithCallback(
                confirm_callback,
                MessageBox,
                message,
                MessageBox.TYPE_YESNO
            )

        except Exception as e:
            logger.error(f"Error deleting manual mapping: {str(e)}")

    def _perform_delete_mapping(self, mapping_to_delete):
        """Actually delete the mapping from database"""
        try:
            manual_db = self.epg_mapper.manual_db
            data = manual_db.load_database()
            mappings = data.get('mappings', [])

            # Find and remove the mapping
            new_mappings = []
            deleted = False

            for mapping in mappings:
                if (mapping.get('channel_name') == mapping_to_delete.get('channel_name') and
                        mapping.get('clean_name') == mapping_to_delete.get('clean_name') and
                        mapping.get('assigned_sref') == mapping_to_delete.get('assigned_sref')):
                    deleted = True
                    continue
                new_mappings.append(mapping)

            if deleted:
                data['mappings'] = new_mappings
                data['last_updated'] = strftime("%Y-%m-%d %H:%M:%S")

                # Save updated database
                with open(manual_db.db_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"✅ Deleted manual mapping: {mapping_to_delete.get('channel_name')}")
                return True

            return False

        except Exception as e:
            logger.error(f"Error performing delete: {str(e)}")
            return False

    def _perform_clean_database(self):
        """Actually clear the entire database"""
        try:

            manual_db = self.epg_mapper.manual_db
            empty_data = manual_db._get_default_structure()
            with open(manual_db.db_path, 'w', encoding='utf-8') as f:
                json.dump(empty_data, f, indent=2, ensure_ascii=False)

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("✅ Manual database cleared")
            return True

        except Exception as e:
            logger.error(f"Error performing database clean: {str(e)}")
            return False

    def _perform_database_import(self, import_path):
        """Perform the actual database import"""
        try:
            if not exists(import_path):
                self.session.openWithCallback(
                    lambda result=None: self._show_enhanced_tools_menu(),
                    MessageBox,
                    _("Import file not found:\n{}").format(import_path),
                    MessageBox.TYPE_ERROR
                )
                return

            # Read import file
            with open(import_path, 'r', encoding='utf-8') as f:
                import_data = json.load(f)

            # Validate structure
            if not isinstance(import_data, dict) or 'mappings' not in import_data:
                self.session.openWithCallback(
                    lambda result=None: self._show_enhanced_tools_menu(),
                    MessageBox,
                    _("Invalid database file structure"),
                    MessageBox.TYPE_ERROR
                )
                return

            # Load current database
            if hasattr(self, 'epg_mapper') and self.epg_mapper:
                current_data = self.epg_mapper.manual_db.load_database()

                # Show import options
                import_mappings = import_data.get('mappings', [])
                current_mappings = current_data.get('mappings', [])

                self._show_import_options(import_path, import_mappings, current_mappings)
            else:
                self.session.openWithCallback(
                    lambda result=None: self._show_enhanced_tools_menu(),
                    MessageBox,
                    _("EPG mapper not available"),
                    MessageBox.TYPE_ERROR
                )

        except Exception as e:
            logger.error(f"Error performing database import: {str(e)}")
            self.session.openWithCallback(
                lambda result=None: self._show_enhanced_tools_menu(),
                MessageBox,
                _("Error importing database: {}").format(str(e)),
                MessageBox.TYPE_ERROR
            )

    def _finalize_import(self, import_path, mode):
        """Finalize the import operation"""
        try:
            if not hasattr(self, 'epg_mapper') or not self.epg_mapper:
                self._show_enhanced_tools_menu()
                return

            with open(import_path, 'r', encoding='utf-8') as f:
                import_data = json.load(f)

            current_data = self.epg_mapper.manual_db.load_database()
            current_mappings = current_data.get('mappings', [])
            import_mappings = import_data.get('mappings', [])

            if mode == "replace":
                # Replace all current mappings
                final_mappings = import_mappings
                action = _("replaced")
            else:  # merge
                # Merge mappings (avoid duplicates by clean_name)
                existing_clean_names = {m.get('clean_name', '') for m in current_mappings}
                new_mappings = [m for m in import_mappings if m.get('clean_name', '') not in existing_clean_names]
                final_mappings = current_mappings + new_mappings
                action = _("merged")

            # Update database
            current_data['mappings'] = final_mappings
            current_data['last_updated'] = strftime("%Y-%m-%d %H:%M:%S")
            current_data['import_source'] = basename(self.epg_mapper.manual_db.db_path)
            current_data['import_date'] = strftime("%Y-%m-%d %H:%M:%S")

            # Use save_database that manages backups intelligently
            success = self.epg_mapper.manual_db.save_database(current_data)

            if success:
                message = _(
                    "Database import successful!\n\n"
                    "{} mappings {}.\n"
                    "Total mappings now: {}\n"
                    "Mode: {}"
                ).format(
                    len(import_mappings), action, len(final_mappings), mode
                )

                self.session.openWithCallback(
                    lambda result=None: self._show_enhanced_tools_menu(),
                    MessageBox,
                    message,
                    MessageBox.TYPE_INFO
                )
            else:
                self.session.openWithCallback(
                    lambda result=None: self._show_enhanced_tools_menu(),
                    MessageBox,
                    _("Error saving imported database"),
                    MessageBox.TYPE_ERROR
                )

        except Exception as e:
            logger.error(f"Error finalizing import: {str(e)}")
            self.session.openWithCallback(
                lambda result=None: self._show_enhanced_tools_menu(),
                MessageBox,
                _("Error finalizing import: {}").format(str(e)),
                MessageBox.TYPE_ERROR
            )

    def _show_import_preview(self, current_mappings, import_mappings, import_path):
        """Show preview of what will be imported"""
        try:
            current_names = {m.get('channel_name', '') for m in current_mappings}
            new_mappings = [m for m in import_mappings if m.get('channel_name', '') not in current_names]
            duplicate_mappings = [m for m in import_mappings if m.get('channel_name', '') in current_names]

            preview_lines = [
                _("📊 IMPORT PREVIEW"),
                _("File: {}").format(basename(import_path)),
                "",
                _("📈 Statistics:"),
                _("• Total in import file: {}").format(len(import_mappings)),
                _("• New mappings: {}").format(len(new_mappings)),
                _("• Duplicate mappings: {}").format(len(duplicate_mappings)),
                _("• Current total: {}").format(len(current_mappings)),
                "",
                _("🆕 New mappings ({}):").format(len(new_mappings))
            ]

            # Show first 5 new mappings
            for i, mapping in enumerate(new_mappings[:5]):
                preview_lines.append(f"  {i + 1}. {mapping.get('channel_name', 'Unknown')}")

            if len(new_mappings) > 5:
                preview_lines.append(f"  ... and {len(new_mappings) - 5} more")

            preview_lines.extend([
                "",
                _("🔄 Duplicates ({}):").format(len(duplicate_mappings))
            ])

            # Show first 5 duplicates
            for i, mapping in enumerate(duplicate_mappings[:5]):
                preview_lines.append(f"  {i + 1}. {mapping.get('channel_name', 'Unknown')}")

            if len(duplicate_mappings) > 5:
                preview_lines.append(f"  ... and {len(duplicate_mappings) - 5} more")

            def preview_closed(result=None):
                self._show_import_options(import_path, import_mappings, current_mappings)

            self.session.openWithCallback(
                preview_closed,
                MessageBox,
                "\n".join(preview_lines),
                MessageBox.TYPE_INFO
            )

        except Exception as e:
            logger.error(f"Error showing import preview: {str(e)}")
            self._show_enhanced_tools_menu()

    def _show_import_options(self, import_path, import_mappings, current_mappings):
        """Show import options (replace/merge/preview)"""
        options = [
            (_("🔄 Replace All ({} mappings)").format(len(import_mappings)), "replace"),
            (_("➕ Merge (keep both)"), "merge"),
            (_("📊 Preview Changes"), "preview"),
            (_("🔙 Back"), "back")
        ]

        def import_option_selected(choice):
            if not choice or choice[1] == "back":
                self._show_enhanced_tools_menu()
                return

            if choice[1] == "replace":
                self._finalize_import(import_path, "replace")
            elif choice[1] == "merge":
                self._finalize_import(import_path, "merge")
            elif choice[1] == "preview":
                self._show_import_preview(current_mappings, import_mappings, import_path)

        self.session.openWithCallback(
            import_option_selected,
            ChoiceBox,
            title=_("Import {} mappings from {}").format(len(import_mappings), basename(import_path)),
            list=options
        )

    def _format_file_size(self, size_bytes):
        """Format file size to human readable format."""
        if size_bytes == 0:
            return "0 B"

        size_units = ["B", "KB", "MB", "GB"]
        unit_index = 0
        size = size_bytes

        while size >= 1024 and unit_index < len(size_units) - 1:
            size /= 1024
            unit_index += 1

        return f"{size:.2f} {size_units[unit_index]}"

    def _handle_file_selection(self, selected_file=None):
        """Handle file selection with robust error handling."""
        if not selected_file or not isinstance(selected_file, (str, list, tuple)):
            self["status"].setText(_("Invalid selection"))
            return

        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug("=== FILE SELECTION STARTED ===")

            # Reset all states
            self.file_loaded = False
            self.m3u_channels_list = []
            self["status"].setText(_("Processing selection..."))

            # Validate input
            if not selected_file:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug("No file selected")
                self["status"].setText(_("No file selected"))
                return

            # Get normalized path
            try:
                if isinstance(selected_file, (tuple, list)) and selected_file:
                    selected_path = normpath(str(selected_file[0]))
                else:
                    selected_path = normpath(str(selected_file))

                if not exists(selected_path):
                    raise IOError(_("File not found"))
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"Path error: {str(e)}")
                self._show_error_message(str(e))
                return

            # DEBUG: Before processing
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"Processing file: {selected_path}")

            # Process file based on conversion type
            path = selected_file[0] if isinstance(selected_file, (list, tuple)) else selected_file
            self.selected_file = normpath(str(path))

            try:
                # AGGIUNTA CRUCIALE: Gestione esplicita per tv_to_tv
                if self.conversion_type == "tv_to_tv":
                    self._parse_tv_file(selected_path)  # Questo è il metodo mancante!
                elif self.conversion_type == "m3u_to_tv":
                    self._parse_m3u_file(selected_path)
                elif self.conversion_type == "tv_to_m3u":
                    self._parse_tv_file(selected_path)
                elif self.conversion_type == "m3u_to_json":
                    self._parse_m3u_file(selected_path)
                elif self.conversion_type == "json_to_m3u":
                    self._parse_json_file(selected_path)
                elif self.conversion_type == "json_to_tv":
                    self._parse_json_file(selected_path)

                # Update state
                self.file_loaded = True
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"File loaded successfully, channels: {len(self.m3u_channels_list)}")

            except Exception as e:
                logger.error(f"Processing failed: {str(e)}")
                raise

        except Exception as e:
            logger.error(f"File selection failed: {str(e)}")
            self.file_loaded = False
            self.m3u_channels_list = []
            self._show_error_message(str(e))
        finally:
            logger.debug("=== FILE SELECTION COMPLETE ===")

    def update_main_bouquet(self, groups):
        """Use CoreConverter for main bouquet update"""
        return self.core_converter.update_main_bouquet(groups)

    def _update_ui_success(self, channel_count):
        """Update UI only if necessary."""
        if not hasattr(self, '_last_channel_count') or self._last_channel_count != channel_count:
            self._last_channel_count = channel_count

            conversion_ready_texts = {
                "tv_to_tv": _("Convert TV to TV"),
                "m3u_to_tv": _("Convert M3U to TV"),
                "tv_to_m3u": _("Convert TV to M3U"),
                "json_to_tv": _("Convert JSON to TV"),
                "json_to_m3u": _("Convert JSON to M3U"),
                "xspf_to_m3u": _("Convert XSPF to M3U"),
                "m3u_to_json": _("Convert M3U to JSON")
            }

            ready_text = conversion_ready_texts.get(self.conversion_type, _("Ready to convert"))
            self["key_green"].setText(ready_text)

            self["key_yellow"].setText(_("Match Editor"))

            if self.conversion_type in ["tv_to_tv", "m3u_to_tv", "json_to_tv"]:
                self["key_yellow"].setText(_("Match Editor"))
            else:
                self["key_yellow"].setText("")

            self["status"].setText(_("Loaded first %d channels. Press Green to convert.") % channel_count)

    def _process_url(self, url):
        """Process URLs correctly - handle already encoded URLs."""
        if not url:
            return url

        original_url = url

        # Check if URL is already encoded
        if '%3a' or '%3A' in url or '%20' in url:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"🔗 URL already encoded: {url[:20]}...")
            return url

        # Apply encoding if not already encoded
        url = url.replace(":", "%3a")
        url = url.replace(" ", "%20")

        if url != original_url:
            logger.debug(f"🔗 URL encoded: {original_url[:20]}... -> {url[:20]}...")

        if config.plugins.m3uconverter.hls_convert.value:
            if any(url.lower().endswith(ext) for ext in ('.m3u8', '.stream')):
                url = f"hls://{url}"

        return url

    def write_group_bouquet(self, group, channels):
        """Use CoreConverter for bouquet writing"""
        safe_name = self.core_converter.get_safe_filename(group)
        return self.core_converter.write_group_bouquet(safe_name, channels, self.epg_mapper)

    def remove_suffixes(self, name):
        """Use CoreConverter for suffix removal"""
        return self.core_converter.remove_suffixes(name)

    def get_safe_filename(self, name):
        """Use CoreConverter for filename generation"""
        return self.core_converter.get_safe_filename(name)

    def get_output_filename(self):
        """Generate a unique file name for export."""
        timestamp = strftime("%Y%m%d_%H%M%S")
        return f"{ARCHIMEDE_M3U_PATH}/archimede_export_{timestamp}.m3u"

    def handle_very_large_file(self, filename):
        """Handle extremely large M3U files with higher limits."""
        file_size_mb = getsize(filename) / (1024 * 1024)

        # Higher limits for large files:
        if file_size_mb > 100:  # Very large files > 100MB
            max_channels = 20000
        elif file_size_mb > 50:   # Large files 50-100MB
            max_channels = 15000
        elif file_size_mb > 20:   # Medium files 20-50MB
            max_channels = 10000
        else:                     # Small files but above threshold
            max_channels = 5000

        self["status"].setText(_("Large file detected: {:.1f}MB. Processing first {} channels...").format(
            file_size_mb, max_channels))

        entries = []
        count = 0

        try:
            with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
                current_params = {}
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    if count >= max_channels:
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info(f"Reached maximum channel limit: {max_channels}")
                        break

                    if line.startswith('#EXTINF:'):
                        current_params = {'f_type': 'inf', 'title': '', 'uri': ''}
                        parts = line[8:].split(',', 1)
                        if len(parts) > 1:
                            current_params['title'] = parts[1].strip()

                    elif line.startswith('#EXTGRP:'):
                        current_params['group-title'] = line[8:].strip()

                    elif line.startswith('#'):
                        continue

                    else:
                        if current_params and line:
                            current_params['uri'] = line.strip()
                            if current_params.get('title'):
                                entries.append(current_params.copy())
                                count += 1
                            current_params = {}

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Large file processing completed: {} channels from {:.1f}MB file".format(
                    len(entries), file_size_mb))

        except Exception as e:
            logger.error(f"Large file processing error: {str(e)}")

        return entries

    def _parse_m3u_file(self, filename=None):
        """Parse M3U file with configurable large file handling."""
        try:
            file_to_parse = filename or self.selected_file
            if not file_to_parse:
                raise ValueError(_("No file selected"))

            file_size = getsize(file_to_parse)
            threshold_bytes = config.plugins.m3uconverter.large_file_threshold_mb.value * 1024 * 1024

            # Use appropriate parsing method based on file size
            if file_size > threshold_bytes:  # > configured MB
                self.m3u_channels_list = self.handle_very_large_file(file_to_parse)
            elif file_size > 1 * 1024 * 1024:  # > 1MB
                self.m3u_channels_list = self._parse_m3u_incremental(file_to_parse)
            else:
                with open(file_to_parse, 'r', encoding='utf-8', errors='replace') as f:
                    data = f.read()
                self.m3u_channels_list = self._parse_m3u_content(data)

            # Filter and process channels with proper attribute mapping
            filtered_channels = []
            for channel in self.m3u_channels_list:
                if channel.get('uri'):
                    # Map attributes to consistent names
                    filtered_channels.append({
                        'name': channel.get('title', ''),
                        'url': self._process_url(channel['uri']),
                        'group': channel.get('group-title', ''),
                        'tvg_id': channel.get('tvg-id', ''),
                        'tvg_name': channel.get('tvg-name', ''),
                        'logo': channel.get('tvg-logo', ''),
                        'duration': channel.get('length', '-1'),
                        'user_agent': channel.get('user-agent', ''),
                        'language': channel.get('tvg-language', '')
                    })

            self.m3u_channels_list = filtered_channels

            # Update UI
            display_list = []
            for idx, channel in enumerate(self.m3u_channels_list[:100]):  # Show only first 100
                name = sub(r'\[.*?\]', '', channel['name']).strip()
                group = channel.get('group', 'Default')
                group = clean_group_name(group)
                display_text = f"{idx + 1:03d}. {group + ' - ' if group else ''}{name}"
                display_list.append(display_text)

            self["list"].setList(display_list)
            self.file_loaded = True

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"✅ FINAL COUNT: {len(self.m3u_channels_list)} channels ready for conversion")
            self._update_ui_success(len(self.m3u_channels_list))

        except Exception as e:
            logger.error(f"❌ Error parsing M3U: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            self.file_loaded = False
            self.m3u_channels_list = []

    def _parse_m3u_content(self, data):
        """Advanced parser for M3U content with support for extended attributes."""
        entries = []
        current_params = {}
        attributes = {}
        lines_processed = 0
        lines = data.split('\n')
        for line in lines:
            lines_processed += 1
            line = line.strip()
            if not line:
                continue

            if lines_processed % 100 == 0:
                eTimer().start(10, True)

            if line.startswith('#EXTM3U'):
                continue

            elif line.startswith('#EXTINF:'):
                current_params = {'f_type': 'inf', 'title': '', 'uri': ''}
                extinf_content = line[8:].strip()

                if ',' in extinf_content:
                    last_comma_index = extinf_content.rfind(',')
                    attributes_part = extinf_content[:last_comma_index].strip()
                    title_part = extinf_content[last_comma_index + 1:].strip()
                    current_params['title'] = title_part
                    attr_matches = findall(r'(\S+?)="([^"]*)"', attributes_part)
                    for key, value in attr_matches:
                        attributes[key.lower()] = value

                    duration_match = search(r'^(-?\d+)', attributes_part)
                    if duration_match:
                        attributes['length'] = duration_match.group(1)

                    current_params.update(attributes)

                else:
                    current_params['title'] = extinf_content

                common_attributes = ['tvg-id', 'tvg-name', 'tvg-logo', 'group-title', 'tvg-language']
                for attr in common_attributes:
                    if attr in attributes:
                        current_params[attr] = attributes[attr]

            elif line.startswith('#EXTGRP:'):
                current_params['group-title'] = str(line[8:].strip())

            elif line.startswith('#EXTVLCOPT:'):
                opt_line = line[11:].strip()
                if '=' in opt_line:
                    key, value = opt_line.split('=', 1)
                    key = key.lower().strip()
                    if key == 'http-user-agent':
                        current_params['user-agent'] = str(value.strip())

            elif line.startswith('#'):
                continue

            elif not line.startswith('#'):
                if current_params and line:
                    current_params['uri'] = str(line.strip())
                    if current_params.get('title'):
                        for key in list(current_params.keys()):
                            if isinstance(current_params[key], list):
                                current_params[key] = str(current_params[key][0]) if current_params[key] else ''

                        entries.append(current_params.copy())
                    current_params = {}
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Parsing completed: {len(entries)} channels found")
        return entries

    def _parse_m3u_incremental(self, filename, chunk_size=65536):
        """Parse M3U file incrementally with better performance for large files."""
        entries = []
        current_params = {}
        buffer = ""
        count = 0
        max_channels = 50000  # Very high limit for incremental parsing

        try:
            with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break

                    buffer += chunk
                    lines = buffer.split('\n')

                    # Keep last incomplete line in buffer
                    if buffer and not buffer.endswith('\n'):
                        buffer = lines.pop()
                    else:
                        buffer = ""

                    for line in lines:
                        if count >= max_channels:
                            break

                        line = line.strip()
                        if not line:
                            continue

                        # Yield control periodically
                        if len(entries) % 50 == 0:
                            eTimer().start(5, True)

                        if line.startswith('#EXTINF:'):
                            current_params = {'f_type': 'inf', 'title': '', 'uri': ''}
                            extinf_content = line[8:].strip()

                            if ',' in extinf_content:
                                last_comma_index = extinf_content.rfind(',')
                                attributes_part = extinf_content[:last_comma_index].strip()
                                title_part = extinf_content[last_comma_index + 1:].strip()

                                current_params['title'] = title_part

                                # Parse attributes
                                attr_matches = findall(r'(\S+?)="([^"]*)"', attributes_part)
                                for key, value in attr_matches:
                                    current_params[key.lower()] = value

                                # Get duration
                                duration_match = search(r'^(-?\d+)', attributes_part)
                                if duration_match:
                                    current_params['length'] = duration_match.group(1)
                            else:
                                current_params['title'] = extinf_content

                        elif line.startswith('#EXTGRP:'):
                            current_params['group-title'] = line[8:].strip()

                        elif line.startswith('#'):
                            continue

                        else:
                            if current_params and line:
                                current_params['uri'] = line.strip()
                                if current_params.get('title'):
                                    entries.append(current_params.copy())
                                current_params = {}

        except Exception as e:
            logger.error(f"Incremental parsing error: {str(e)}")

        return entries

    def _parse_tv_file(self, filename=None):
        """Parse TV bouquet file for TV-to-TV conversion."""
        try:
            file_to_parse = filename or self.selected_file
            if not file_to_parse:
                raise ValueError(_("No file selected"))

            channels = []
            with codecs.open(file_to_parse, "r", encoding="utf-8") as f:
                content = f.read()

                # Enhanced pattern to handle different TV bouquet formats
                pattern = r'#SERVICE\s(?:4097|5002):[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:[^:]*:(.*?)\s*\n#DESCRIPTION\s*(.*?)\s*\n'
                matches = findall(pattern, content, DOTALL)

                for service, name in matches:
                    # URL decoding and filtering HTTP/HTTPS/HLS streams
                    url = unquote(service.strip())
                    if any(url.startswith(proto) for proto in ('http://', 'https://', 'hls://')):
                        # Store as tuple for TV bouquets (name, url)
                        channel_data = (name.strip(), url)
                        channels.append(channel_data)

            if not channels:
                raise ValueError(_("No IPTV channels found in the bouquet"))

            self.m3u_channels_list = channels

            # Update UI with channel names only
            display_list = [channel[0] for channel in channels]
            self["list"].setList(display_list)
            self.file_loaded = True

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"✅ TV file parsed successfully: {len(channels)} channels found")
                logger.info(f"📺 Sample channels: {channels[:3]}")

            self._update_ui_success(len(self.m3u_channels_list))

        except Exception as e:
            logger.error(f"❌ Error parsing TV bouquet: {str(e)}")
            self.file_loaded = False
            self.m3u_channels_list = []
            self.session.open(
                MessageBox,
                _("Error parsing TV bouquet file:\n%s") % str(e),
                MessageBox.TYPE_ERROR,
                timeout=6
            )

    def _parse_json_file(self, filename=None):
        """Parse JSON file containing channel information."""
        file_to_parse = filename or self.selected_file
        try:
            with open(file_to_parse, 'r', encoding='utf-8') as f:
                data = json.load(f)

            self.m3u_channels_list = []
            channels = []
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"JSON data type: {type(data)}")
            if isinstance(data, dict):
                logger.debug(f"JSON keys: {list(data.keys())}")

            # Handle different JSON structures
            if isinstance(data, dict):
                # Try various common JSON structures
                if 'channels' in data and isinstance(data['channels'], list):
                    channels = data['channels']
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("Found channels in 'channels' key")
                elif 'playlist' in data and isinstance(data['playlist'], list):
                    channels = data['playlist']
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("Found channels in 'playlist' key")
                elif 'items' in data and isinstance(data['items'], list):
                    channels = data['items']
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("Found channels in 'items' key")
                elif 'streams' in data and isinstance(data['streams'], list):
                    channels = data['streams']
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("Found channels in 'streams' key")
                elif 'data' in data and isinstance(data['data'], list):
                    channels = data['data']
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug("Found channels in 'data' key")
                else:
                    # If no specific key found, try to use all values that are lists
                    for key, value in data.items():
                        if isinstance(value, list):
                            channels = value
                            if config.plugins.m3uconverter.enable_debug.value:
                                logger.debug(f"Using list from key: {key}")
                            break

            elif isinstance(data, list):
                # Direct array of channels
                channels = data
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug("JSON is direct array of channels")
            else:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("Unsupported JSON structure")
                raise ValueError("Unsupported JSON structure")

            # Process channels
            for channel in channels:
                if not isinstance(channel, dict):
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.warning(f"Skipping non-dict channel: {channel}")
                    continue

                # Extract channel information with flexible field names
                name = (channel.get('name') or channel.get('title') or
                        channel.get('channel') or channel.get('channel_name') or 'Unknown')

                url = (channel.get('url') or channel.get('link') or channel.get('stream') or
                       channel.get('source') or channel.get('address') or '')

                # Decode URL if it's encoded (check for % encoding)
                if url and '%' in url:
                    try:
                        url = unquote(url)
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.debug(f"Decoded URL: {url}")
                    except Exception as e:
                        logger.error(f"Error decoding URL: {str(e)}")

                group = (channel.get('group') or channel.get('category') or
                         channel.get('group-title') or channel.get('group_title') or '')

                logo = (channel.get('logo') or channel.get('icon') or
                        channel.get('tvg-logo') or channel.get('tvg_logo') or '')

                tvg_id = (channel.get('tvg-ID') or channel.get('tvg_id') or
                          channel.get('id') or channel.get('channel_id') or '')

                tvg_name = channel.get('tvg-name') or channel.get('tvg_name') or name

                # Check if URL is valid after decoding
                is_valid_url = False
                if url:
                    # Check for various URL protocols
                    url_protocols = ('http://', 'https://', 'rtsp://', 'rtmp://', 'udp://', 'rtp://')
                    is_valid_url = any(url.startswith(proto) for proto in url_protocols)

                    # Also check for encoded URLs that might become valid after decoding
                    if not is_valid_url and '%' in url:
                        decoded_url = unquote(url)
                        is_valid_url = any(decoded_url.startswith(proto) for proto in url_protocols)
                        if is_valid_url:
                            url = decoded_url

                group = clean_group_name(group)
                # Only add channels with a valid URL
                if is_valid_url:
                    self.m3u_channels_list.append({
                        'name': name,
                        'url': url,
                        'group': group,
                        'logo': logo,
                        'tvg_id': tvg_id,
                        'tvg_name': tvg_name,
                        'duration': channel.get('duration', '-1'),
                        'user_agent': channel.get('user-agent') or channel.get('user_agent') or '',
                        'program_id': channel.get('program-id') or channel.get('program_id') or ''
                    })
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.debug(f"Added channel: {name} - {url}")
                else:
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.warning(f"Skipping channel without valid URL: {name} - URL: {url}")

            # Update UI based on conversion type
            display_list = []
            for channel in self.m3u_channels_list:
                name = sub(r'\[.*?\]', '', channel['name']).strip()
                group = channel.get('group', '')
                display_list.append(f"{group + ' - ' if group else ''}{name}")

            # Update the list immediately
            self["list"].setList(display_list)
            self.file_loaded = True

            self._update_ui_success(len(self.m3u_channels_list))

            # Log results for debugging
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"Found {len(self.m3u_channels_list)} channels in JSON file")
            if len(self.m3u_channels_list) > 0:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"Sample channel: {self.m3u_channels_list[0]}")

        except Exception as e:
            logger.error(f"Error parsing JSON: {str(e)}")
            self.file_loaded = False
            self.m3u_channels_list = []
            self.session.open(
                MessageBox,
                _("Error parsing JSON file. Please check the format.\n\nError: %s") % str(e),
                MessageBox.TYPE_ERROR,
                timeout=6
            )

    def _real_conversion_task(self, m3u_path=None, progress_callback=None):
        """Optimized conversion with batch processing"""
        # DEBUG: Check the status of the epg_mapper
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"🔍 EPG_MAPPER STATUS: exists={hasattr(self, 'epg_mapper')}, value={self.epg_mapper if hasattr(self, 'epg_mapper') else 'NO ATTR'}")

        # DEBUG: Detailed channel analysis
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info("🔍 CHANNEL ANALYSIS START")
            for i, ch in enumerate(self.m3u_channels_list[:10]):  # First 10 channels
                logger.info(f"   {i}: '{ch.get('name', 'Unknown')}' -> URL: {ch.get('url', 'NO URL')}")
            logger.info(f"📊 TOTAL CHANNELS TO PROCESS: {len(self.m3u_channels_list)}")

        if self.cancel_conversion:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("🛑 Conversion cancelled before starting")
            return (False, "Conversion cancelled before start")

        # RESET STATISTICS ONLY FOR NEW CONVERSION - don't reset cache
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            # Reset only statistics, not cache
            self.epg_mapper.reset_caches(clear_match_cache=False, reset_stats=True)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("🔄 Statistics reset for new conversion")

        # SIMPLIFIED INITIAL CHECK
        if not m3u_path and not self.selected_file:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error("No file selected for conversion")
            return (False, "No file selected")

        # EPG MAPPER INITIALIZATION (only if needed)
        if not hasattr(self, 'epg_mapper') or not self.epg_mapper:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("🔄 Initializing EPG mapper before conversion...")
            self.epg_mapper = self._initialize_epg_mapper()

        if not self.epg_mapper:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.error("❌ EPG mapper initialization failed")
            return (False, "EPG mapper not initialized")

        # RYTEC DATABASE CHECK
        rytec_count = len(self.epg_mapper.mapping.rytec.get('basic', {}))
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"🔍 RYTEC DATABASE STATUS: {rytec_count} channels loaded")

        # Preload caches for performance
        if rytec_count == 0:
            logger.warning("⚠️ Rytec database is EMPTY")
            # Do not reinitialize - if it's empty, it's empty

        # PRELOAD CACHES (only once)
        self.epg_mapper.optimize_matching()

        try:
            file_to_parse = m3u_path or self.selected_file
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"Starting conversion for: {file_to_parse}")
            if not file_to_parse:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("No file selected for conversion")
                return (False, "No file selected")

            # Extract EPG URL from M3U file
            epg_url = None
            try:
                epg_url = self.epg_mapper._extract_epg_url_from_m3u(file_to_parse)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"Extracted EPG URL: {epg_url}")
            except Exception as e:
                logger.warning(f"Error extracting EPG URL: {str(e)}")
                epg_url = None

            # Parse file if not already parsed
            if file_to_parse and not self.m3u_channels_list:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"Parsing file: {file_to_parse}")
                with open(file_to_parse, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                self.m3u_channels_list = self._parse_m3u_content(content)

                # Use the correct field mapping for M3U files
                normalized_list = []
                for ch in self.m3u_channels_list:
                    # Map M3U attributes to consistent names - FIXED to use 'title' and 'uri'
                    if ch.get('uri'):
                        normalized_list.append({
                            'name': ch.get('title', ''),
                            'url': ch.get('uri', ''),
                            'group': ch.get('group-title', ''),
                            'tvg_id': ch.get('tvg-id', ''),
                            'tvg_name': ch.get('tvg-name', ''),
                            'logo': ch.get('tvg-logo', ''),
                            'user_agent': ch.get('user_agent', ''),
                            'program_id': ch.get('program-id', '')
                        })
                self.m3u_channels_list = normalized_list

            total_original = len(self.m3u_channels_list)
            if total_original == 0:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("No valid channels found after parsing")
                return (False, "No valid channels found")

            # VALID CHANNELS ONLY - FIXED COUNTING
            processed_count = 0
            processed_channels = set()
            valid_channels = []

            for ch in self.m3u_channels_list:
                url = ch.get('url', '')
                if url and len(url) > 10:
                    # Use channel name + URL as unique identifier
                    channel_id = f"{ch.get('name', '')}_{url}"
                    if channel_id not in processed_channels:
                        processed_channels.add(channel_id)
                        valid_channels.append(ch)

            total_valid = len(valid_channels)
            total_original = len(self.m3u_channels_list)

            # Store the REAL count for statistics
            if hasattr(self, 'epg_mapper') and self.epg_mapper:
                self.epg_mapper._last_processed_count = total_valid

            if total_valid == 0:
                logger.error("❌ No valid channels with URLs found")
                return (False, "No valid channels with URLs")

            batch_size = 50
            groups = {}
            epg_data = []
            stats = {
                'total_channels': total_valid,
                'total_original_channels': total_original,
                'rytec_matches': 0,
                'dvb_matches': 0,
                'dvbt_matches': 0,
                'fallback_matches': 0,
                'manual_db_matches': 0,
                'consistent_fallback': 0,
                'batch_processed': 0
            }

            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"Starting optimized conversion with {total_valid} valid channels (originally {total_original}) in batches of {batch_size}")

            # USE ONLY VALID CHANNELS - FIXED COUNTING
            for batch_start in range(0, total_valid, batch_size):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"=== STARTING BATCH {batch_start // batch_size + 1} ===")
                if self.cancel_conversion:
                    logger.info("🛑 Conversion cancelled during batch processing")
                    return (False, "Conversion cancelled during processing")

                batch_end = min(batch_start + batch_size, total_valid)
                batch_channels = valid_channels[batch_start:batch_end]

                # Process the batch with CORRECT counting
                for idx, channel in enumerate(batch_channels):
                    if self.cancel_conversion:
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info("🛑 Conversion cancelled during channel processing")
                        return (False, "Conversion cancelled during channel processing")

                    # CORRECT: processed_count starts from batch_start + current index
                    processed_count = batch_start + idx + 1

                    # Get channel info
                    name = channel.get('name', 'Unknown')
                    url = channel.get('url', '')
                    tvg_id = channel.get('tvg_id', '')
                    original_name = name

                    # USE CONSISTENT MATCHING APPROACH
                    clean_name = self.epg_mapper.clean_channel_name(name, preserve_variants=False)

                    # ENHANCED: Special handling for short names and numbered channels
                    if len(clean_name) <= 5 or any(char.isdigit() for char in clean_name):
                        # Try enhanced search first for short names
                        enhanced_matches = self.epg_mapper._enhanced_search_short_names(clean_name, original_name)
                        if enhanced_matches:
                            # Use the best enhanced match
                            best_enhanced = max(enhanced_matches, key=lambda x: (x.get('priority', 0), x['similarity']))
                            service_ref = best_enhanced['sref']
                            match_type = f"{best_enhanced['type']}_enhanced"
                        else:
                            # Fall back to normal matching
                            service_ref, match_type = self.epg_mapper._find_best_service_match(
                                clean_name, tvg_id, original_name, channel['url']
                            )
                    else:
                        # Normal matching for longer names
                        service_ref, match_type = self.epg_mapper._find_best_service_match(
                            clean_name, tvg_id, original_name, channel['url']
                        )

                    # DETAILED DEBUG
                    if config.plugins.m3uconverter.enable_debug.value and idx < 10:  # Only first 10 channels
                        self.epg_mapper._debug_matching_process(original_name, clean_name, tvg_id, service_ref, match_type)

                    # DEBUG: Check that service_ref is not None
                    if service_ref is None and config.plugins.m3uconverter.enable_debug.value:
                        logger.warning(f"❌ service_ref is None for: '{original_name}', match_type: {match_type}")

                    bouquet_sref = self.epg_mapper._generate_hybrid_sref(service_ref, url, for_epg=False)
                    channel['original_service_ref'] = service_ref
                    channel['sref'] = bouquet_sref
                    channel['match_type'] = match_type

                    if service_ref and service_ref.startswith('1:'):
                        epg_sref = service_ref
                    else:
                        epg_sref = bouquet_sref  # Fallback to IPTV

                    epg_entry = {
                        'tvg_id': tvg_id or name,
                        'sref': epg_sref,
                        'name': name,
                        'url': url,
                        'original_name': original_name,
                        'match_type': match_type
                    }
                    epg_data.append(epg_entry)

                    # Count matches correctly
                    if 'rytec' in match_type:
                        self.epg_mapper._stats_counters['rytec_matches'] += 1
                    elif 'dvb' in match_type:
                        if self.epg_mapper._is_dvb_t_service(service_ref):
                            self.epg_mapper._stats_counters['dvbt_matches'] += 1
                        else:
                            self.epg_mapper._stats_counters['dvb_matches'] += 1
                    elif 'manual_db' in match_type:
                        self.epg_mapper._stats_counters['manual_db_matches'] += 1
                    elif 'consistent_fallback' in match_type:
                        self.epg_mapper._stats_counters['consistent_fallback'] += 1
                    else:
                        self.epg_mapper._stats_counters['fallback_matches'] += 1

                    # processed_count
                    if processed_count % 50 == 0:
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.debug(f"Progress counts: Rytec={stats['rytec_matches']}, DVB={stats['dvb_matches']}, Consistent={stats['consistent_fallback']}, Fallback={stats['fallback_matches']}")

                    # Grouping
                    if config.plugins.m3uconverter.bouquet_mode.value == "single":
                        group = "All Channels"
                    else:
                        group = clean_group_name(channel.get('group', 'Default'))

                    groups.setdefault(group, []).append(channel)

                    # processed_count E total_valid
                    if processed_count % 10 == 0:
                        progress = int((processed_count / total_valid) * 100)
                        progress_message = _(f"Converting: {processed_count}/{total_valid} ({progress}%)")
                        self.update_progress(processed_count, progress_message)

                    # processed_count
                    if config.plugins.m3uconverter.enable_debug.value and processed_count % 20 == 0:
                        logger.info("🔍 CONVERSION DEBUG:")
                        logger.info(f"   Channel: {original_name}")
                        logger.info(f"   Clean: {clean_name}")
                        logger.info(f"   TVG ID: {tvg_id}")
                        logger.debug(f"   service_ref: {service_ref}")
                        logger.debug(f"   generated_sref: {channel['sref']}")
                        logger.debug(f"   match_type: {match_type}")
                        logger.info(f"   Bouquet SREF: {bouquet_sref}")

                stats['batch_processed'] += 1
                time.sleep(0.005)

            # Phase 2: Write bouquets
            bouquet_names = []

            if config.plugins.m3uconverter.bouquet_mode.value == "single":
                all_channels = []
                for group_channels in groups.values():
                    all_channels.extend(group_channels)

                bouquet_name = self.get_safe_filename(basename(file_to_parse).split('.')[0])
                if self.write_group_bouquet(bouquet_name, all_channels):
                    bouquet_names.append(bouquet_name)
            else:
                for group_name, group_channels in groups.items():
                    safe_name = self.get_safe_filename(group_name)
                    if self.write_group_bouquet(safe_name, group_channels):
                        bouquet_names.append(safe_name)

            # Update main bouquet
            if bouquet_names:
                self.update_main_bouquet(bouquet_names)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"Main bouquet updated with {len(bouquet_names)} bouquets")

            # Phase 3: Optimized EPG generation
            if config.plugins.m3uconverter.epg_enabled.value and epg_data:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"EPG enabled, generating optimized files for {len(epg_data)} channels")

                bouquet_name_for_epg = bouquet_names[0] if bouquet_names else "default_bouquet"

                # USE OPTIMIZED EPG GENERATION
                epg_success = self.epg_mapper._generate_epg_channels_file(epg_data, bouquet_name_for_epg)

                if epg_success:
                    # Generate sources file
                    sources_success = self.epg_mapper._generate_epgshare_sources_file(bouquet_name_for_epg, epg_url)

                    if sources_success:
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info("Optimized EPG generation completed successfully")

                            # Verification
                            self.epg_mapper._debug_verify_epg_files(bouquet_name_for_epg)
                    else:
                        logger.warning("EPG sources generation failed")
                else:
                    logger.warning("Optimized EPG channels generation failed")

            # SAVE GOOD MATCHES
            if False and config.plugins.m3uconverter.use_manual_database.value:  # ← DISABLED
                auto_saved_count = self.epg_mapper._save_good_matches(self.m3u_channels_list)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"💾 AUTO-SAVE RESULTS: {auto_saved_count} matches saved to manual database")

            # Analyze data cache
            if config.plugins.m3uconverter.enable_debug.value:
                self.epg_mapper._save_complete_cache_analysis()

            # Performance stats: Count only REAL EPG matches (Rytec + DVB)
            rytec_matches = self.epg_mapper._stats_counters.get('rytec_matches', 0)
            dvb_matches = self.epg_mapper._stats_counters.get('dvb_matches', 0)
            dvbt_matches = self.epg_mapper._stats_counters.get('dvbt_matches', 0)
            fallback_matches = self.epg_mapper._stats_counters.get('fallback_matches', 0)
            manual_db_matches = self.epg_mapper._stats_counters.get('manual_db_matches', 0)
            consistent_fallback = self.epg_mapper._stats_counters.get('consistent_fallback', 0)

            # Calculate the CORRECT total - should equal total_valid
            total_calculated = (rytec_matches + dvb_matches + dvbt_matches +
                                fallback_matches + manual_db_matches + consistent_fallback)

            # VERIFICATION: Log detailed counts for debugging
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("🔍 FINAL COUNT VERIFICATION:")
                logger.info(f"   Total valid channels: {total_valid}")
                logger.info(f"   Total calculated matches: {total_calculated}")
                logger.info(f"   Rytec: {rytec_matches}, DVB-S: {dvb_matches}, DVB-T: {dvbt_matches}")
                logger.info(f"   Manual DB: {manual_db_matches}, Fallback: {fallback_matches}")

            # Check consistency - use the REAL processed count (total_valid)
            if total_calculated != total_valid:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.warning(f"⚠️ COUNT MISMATCH: Calculated {total_calculated} vs Processed {total_valid}")

                # Force consistency by adjusting fallback count
                calculated_without_fallback = total_calculated - fallback_matches
                fallback_matches = max(0, total_valid - calculated_without_fallback)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"🔧 ADJUSTED: Fallback now {fallback_matches}, Total now {calculated_without_fallback + fallback_matches}")
            else:
                logger.info("✅ Count verification PASSED - all channels accounted for")

            # Real EPG: only Rytec + DVB + DVB-T + Manual DB (exclude fallback)
            real_epg_matches = rytec_matches + dvb_matches + dvbt_matches + manual_db_matches

            # Get performance stats safely
            try:
                perf_stats = self.epg_mapper._get_cache_statistics()
            except Exception as e:
                logger.warning(f"Error getting performance stats: {str(e)}")
                perf_stats = {'match_hit_rate': 'N/A', 'match_cache_size': 0}

            # Update stats for consistency
            stats.update({
                'rytec_matches': rytec_matches,
                'dvb_matches': dvb_matches,
                'dvbt_matches': dvbt_matches,
                'fallback_matches': fallback_matches,
                'manual_db_matches': manual_db_matches
            })

            # Print detailed statistics
            self.print_detailed_conversion_stats()

            return (True, total_valid, real_epg_matches, perf_stats, stats)

        except Exception as e:
            logger.error(f"Optimized conversion error: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return (False, str(e))

    def _convert_m3u_to_tv(self):
        """Convert M3U to TV bouquet format - WITH BETTER CANCELLATION"""

        # Double-check we're not already converting
        if self.is_converting:
            self.session.open(MessageBox, _("Conversion already in progress"), MessageBox.TYPE_WARNING)
            return

        # Clear log before starting conversion
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.epg_mapper._cleanup_all_match_types()
            self.epg_mapper._cleanup_log_file()

        def conversion_task():
            try:
                # Check cancellation immediately
                if self.cancel_conversion:
                    return (False, "Conversion cancelled before start")
                if hasattr(self, 'epg_mapper') and self.epg_mapper:
                    self.epg_mapper._refresh_config()
                return self.core_converter.safe_conversion(self._real_conversion_task, self.selected_file, None)
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"Conversion task failed: {str(e)}")
                return (False, str(e))

        # Reset UI and start conversion
        self.reset_conversion_buttons()

        self.is_converting = True
        self.cancel_conversion = False
        self["key_red"].setText("")
        self["key_green"].setText("")
        self["key_blue"].setText(_("Cancel"))

        # Start memory cleanup timer
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.epg_mapper.optimize_memory_timer.start(30000)

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def _convert_tv_to_m3u(self):
        """Convert TV bouquet to M3U format."""
        if self.is_converting:
            self.session.open(MessageBox, _("Conversion already in progress"), MessageBox.TYPE_WARNING)
            return

        # Global cleanup before conversion
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.epg_mapper._cleanup_all_match_types()
            self.epg_mapper._cleanup_log_file()

        def _real_tv_to_m3u_conversion():
            try:
                output_file = self.get_output_filename()
                total_items = len(self.m3u_channels_list)

                with open(output_file, 'w', encoding='utf-8') as f:
                    if self.cancel_conversion:
                        return (False, "Conversion cancelled")

                    f.write('#EXTM3U\n')
                    f.write('#EXTENC: UTF-8\n')
                    f.write(f'#EXTARCHIMEDE: Generated by Archimede Converter {CURRENT_VERSION}\n')

                    for idx, (name, url) in enumerate(self.m3u_channels_list):
                        f.write(f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}",{name}\n')
                        f.write(f'{url}\n')

                        if idx % 10 == 0:
                            progress = (idx + 1) / total_items * 100
                            self.update_progress(
                                idx + 1,
                                _("Converting: {name} ({percent}%)").format(
                                    name=name,
                                    percent=int(progress)
                                )
                            )

                return (True, output_file, total_items)

            except IOError as e:
                return (False, _("File write error: %s") % str(e))
            except Exception as e:
                return (False, _("tv_to_m3u Conversion error: %s") % str(e))

        def conversion_task():
            try:
                # Check cancellation immediately
                if self.cancel_conversion:
                    return (False, "Conversion cancelled before start")

                if hasattr(self, 'epg_mapper') and self.epg_mapper:
                    self.epg_mapper._refresh_config()
                return _real_tv_to_m3u_conversion()
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"Conversion task failed: {str(e)}")
                return (False, str(e))

        # Reset UI and start conversion
        self.reset_conversion_buttons()

        self.is_converting = True
        self.cancel_conversion = False
        self["key_red"].setText("")
        self["key_green"].setText("")
        self["key_blue"].setText(_("Cancel"))

        # Start memory cleanup timer
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.epg_mapper.optimize_memory_timer.start(30000)

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def _convert_tv_to_tv(self):
        """Convert TV bouquet to optimized TV bouquet format"""
        if self.is_converting:
            self.session.open(MessageBox, _("Conversion already in progress"), MessageBox.TYPE_WARNING)
            return

        # Global cleanup before conversion
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.epg_mapper._cleanup_all_match_types()
            self.epg_mapper._cleanup_log_file()

        if not hasattr(self, 'm3u_channels_list') or not self.m3u_channels_list:
            self.session.open(MessageBox, _("No channels loaded. Please select a valid TV bouquet file."), MessageBox.TYPE_ERROR)
            return

        logger.info(f"🎯 Selected file: {self.selected_file}")

        # Extract bouquet name correctly
        bouquet_name = basename(self.selected_file).replace('userbouquet.', '').replace('.tv', '')
        safe_name = self.get_safe_filename(bouquet_name)

        logger.info(f"🎯 Bouquet name: {bouquet_name}")
        logger.info(f"🎯 Safe filename: {safe_name}")

        def _tv_to_tv_conversion():
            try:
                if not self.m3u_channels_list:
                    return (False, "No channels to convert")

                logger.info(f"🔍 DEBUG: m3u_channels_list type: {type(self.m3u_channels_list)}")
                logger.info(f"🔍 DEBUG: m3u_channels_list length: {len(self.m3u_channels_list)}")

                if self.m3u_channels_list:
                    logger.info(f"🔍 DEBUG: First item type: {type(self.m3u_channels_list[0])}")
                    logger.info(f"🔍 DEBUG: First item: {self.m3u_channels_list[0]}")
                    if isinstance(self.m3u_channels_list[0], dict):
                        logger.info(f"🔍 DEBUG: First item keys: {self.m3u_channels_list[0].keys()}")

                # Convert tuple data to dictionary format for EPG processing
                processed_channels = []
                epg_data = []

                for idx, channel in enumerate(self.m3u_channels_list):
                    logger.info(f"🔍 DEBUG: Processing channel {idx}, type: {type(channel)}")

                    if isinstance(channel, tuple) and len(channel) == 2:
                        # Convert (name, url) tuple to dictionary
                        name, url = channel
                        logger.info(f"🔍 DEBUG: Converting tuple - name: '{name}', url: '{url}'")

                        channel_dict = {
                            'name': name,
                            'url': url,
                            'original_name': name
                        }
                        processed_channels.append(channel_dict)
                    elif isinstance(channel, dict):
                        # Already in correct format - CERCHIAMO SIA 'name' CHE 'title'
                        channel_name = channel.get('name') or channel.get('title') or 'Unknown'
                        url = channel.get('url') or channel.get('uri') or ''

                        logger.info(f"🔍 DEBUG: Processing dict - found name: '{channel.get('name')}', title: '{channel.get('title')}', selected: '{channel_name}'")

                        channel_dict = {
                            'name': channel_name,
                            'url': url,
                            'original_name': channel_name
                        }

                        # Copia tutti gli altri campi utili
                        for key, value in channel.items():
                            if key not in ['name', 'title', 'url', 'uri']:
                                channel_dict[key] = value

                        processed_channels.append(channel_dict)
                    else:
                        logger.warning(f"⚠️ Skipping invalid channel format: {type(channel)} - {channel}")
                        continue

                if not processed_channels:
                    return (False, "No valid channels processed")

                logger.info(f"✅ DEBUG: Successfully processed {len(processed_channels)} channels")

                # Process channels with EPG matching
                optimized_channels = []
                for idx, channel in enumerate(processed_channels):
                    if self.cancel_conversion:
                        return (False, "Conversion cancelled")

                    # DEBUG: Check channel structure before processing
                    if not isinstance(channel, dict):
                        logger.error(f"❌ DEBUG: Channel {idx} is not a dict: {type(channel)}")
                        continue

                    name = channel.get('name', '')
                    url = channel.get('url', '')

                    if not name:
                        logger.warning(f"⚠️ DEBUG: Channel {idx} has empty name: {channel}")
                        # Skip channels without name
                        continue

                    logger.info(f"🔍 DEBUG: Processing channel {idx}: '{name}'")

                    # Find better EPG match if available
                    clean_name = self.epg_mapper.clean_channel_name(name)
                    service_ref, match_type = self.epg_mapper._find_best_service_match(
                        clean_name, "", name, url
                    )

                    # Use existing URL but with better service reference if found
                    if service_ref and service_ref.startswith('1:0:'):
                        bouquet_sref = self.epg_mapper._generate_hybrid_sref(service_ref, url, for_epg=False)
                        epg_sref = service_ref  # Per EPG usa il riferimento DVB originale
                    else:
                        bouquet_sref = self.epg_mapper._generate_service_reference(url)
                        epg_sref = bouquet_sref  # Fallback a IPTV

                    optimized_channel = {
                        'name': name,
                        'url': url,
                        'sref': bouquet_sref,
                        'match_type': match_type,
                        'original_name': name,
                        'original_service_ref': service_ref
                    }
                    optimized_channels.append(optimized_channel)

                    # Add to EPG data
                    epg_entry = {
                        'tvg_id': name,  # Use the name as TVG ID
                        'sref': epg_sref,
                        'name': name,
                        'url': url,
                        'original_name': name,
                        'match_type': match_type
                    }
                    epg_data.append(epg_entry)

                    # Update progress
                    if idx % 10 == 0:
                        progress = (idx + 1) / len(processed_channels) * 100
                        self.update_progress(
                            idx + 1,
                            _("Converting: {} ({}%)").format(name, int(progress))
                        )

                # Write optimized bouquet
                if optimized_channels:
                    if self.write_group_bouquet(safe_name, optimized_channels):
                        # Update main bouquet
                        self.update_main_bouquet([safe_name])

                        # Generate EPG files if enabled
                        if config.plugins.m3uconverter.epg_enabled.value and epg_data:
                            logger.info(f"🎯 Generating EPG for TV-to-TV conversion: {len(epg_data)} channels")

                            # Generate channels.xml
                            epg_success = self.epg_mapper._generate_epg_channels_file(epg_data, safe_name)

                            if epg_success:
                                # Generate sources.xml
                                sources_success = self.epg_mapper._generate_epgshare_sources_file(safe_name)

                                if sources_success:
                                    logger.info("✅ EPG files generated successfully for TV-to-TV conversion")
                                else:
                                    logger.warning("⚠️ Failed to generate EPG sources for TV-to-TV conversion")
                            else:
                                logger.warning("⚠️ Failed to generate EPG channels for TV-to-TV conversion")

                        # Store processed channels for potential manual editing
                        self.m3u_channels_list = optimized_channels

                        return (True, safe_name, len(optimized_channels))
                    else:
                        return (False, "Failed to write optimized bouquet")
                else:
                    return (False, "No valid channels to write")

            except Exception as e:
                logger.error(f"TV-to-TV conversion error: {str(e)}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                return (False, str(e))

        def conversion_task():
            try:
                # Check cancellation immediately
                if self.cancel_conversion:
                    return (False, "Conversion cancelled before start")
                if hasattr(self, 'epg_mapper') and self.epg_mapper:
                    self.epg_mapper._refresh_config()
                return core_converter.safe_conversion(_tv_to_tv_conversion)
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"Conversion task failed: {str(e)}")
                return (False, str(e))

        # Reset UI and start conversion
        self.reset_conversion_buttons()

        self.is_converting = True
        self.cancel_conversion = False
        self["key_red"].setText("")
        self["key_green"].setText("")
        self["key_blue"].setText(_("Cancel"))

        # Start memory cleanup timer
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.epg_mapper.optimize_memory_timer.start(30000)

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def _convert_json_to_m3u(self):
        """Convert JSON playlist to M3U format."""
        if self.is_converting:
            self.session.open(MessageBox, _("Conversion already in progress"), MessageBox.TYPE_WARNING)
            return

        # Global cleanup before starting conversion
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.epg_mapper._cleanup_all_match_types()
            self.epg_mapper._cleanup_log_file()

        def conversion_task():
            try:
                if self.cancel_conversion:
                    return (False, "Conversion cancelled before start")

                # MAIN CONVERSION LOGIC
                if not self.m3u_channels_list:
                    self._parse_json_file(self.selected_file)

                if not self.m3u_channels_list:
                    return (False, "No valid channels found in JSON file")

                total_channels = len(self.m3u_channels_list)
                base_name = basename(self.selected_file).split('.')[0]
                output_dir = dirname(self.selected_file)
                output_file = join(output_dir, f"{base_name}.m3u")

                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write('#EXTM3U\n')
                    f.write('#EXTENC: UTF-8\n')
                    f.write(f'#EXTARCHIMEDE: Generated by Archimede Converter {CURRENT_VERSION}\n')

                    for idx, channel in enumerate(self.m3u_channels_list):
                        if not channel.get('url'):
                            continue

                        if self.cancel_conversion:
                            return (False, "Conversion cancelled")

                        # Update progress
                        if idx % 10 == 0:
                            progress = (idx + 1) / total_channels * 100
                            self.update_progress(
                                idx + 1,
                                _("Converting: {name} ({percent}%)").format(
                                    name=channel.get('name', 'Unknown'),
                                    percent=int(progress)
                                )
                            )

                        # Build EXTINF line
                        name = channel.get('name', '')
                        tvg_id = channel.get('tvg_id', '')
                        tvg_name = channel.get('tvg_name', '')
                        tvg_logo = channel.get('logo', '')
                        group = clean_group_name(channel.get('group', ''))
                        duration = channel.get('duration', '-1')

                        extinf = f'#EXTINF:{duration}'
                        if tvg_id:
                            extinf += f' tvg-id="{tvg_id}"'
                        if tvg_name:
                            extinf += f' tvg-name="{tvg_name}"'
                        if tvg_logo:
                            extinf += f' tvg-logo="{tvg_logo}"'
                        if group:
                            extinf += f' group-title="{group}"'
                        extinf += f',{name}\n'

                        f.write(extinf)
                        f.write(channel['url'] + '\n')

                return (True, output_file, total_channels)

            except Exception as e:
                return (False, str(e))

        # Reset UI and start conversion
        self.reset_conversion_buttons()
        self.is_converting = True
        self.cancel_conversion = False
        self["key_red"].setText("")
        self["key_green"].setText("")
        self["key_blue"].setText(_("Cancel"))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def _convert_m3u_to_json(self):
        """Convert M3U playlist to JSON format."""
        if self.is_converting:
            self.session.open(MessageBox, _("Conversion already in progress"), MessageBox.TYPE_WARNING)
            return

        # Global cleanup before conversion
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.epg_mapper._cleanup_all_match_types()
            self.epg_mapper._cleanup_log_file()

        def conversion_task():
            try:
                if self.cancel_conversion:
                    return (False, "Conversion cancelled before start")

                # Parse the M3U file if not already parsed
                if not self.m3u_channels_list:
                    with open(self.selected_file, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    self.m3u_channels_list = self._parse_m3u_content(content)

                    # Normalize M3U entries ⭐⭐
                    normalized_list = []
                    for ch in self.m3u_channels_list:
                        if ch.get('uri'):
                            normalized_list.append({
                                'name': ch.get('title', ''),            # title → name
                                'url': ch.get('uri', ''),
                                'group': ch.get('group-title', ''),
                                'tvg_id': ch.get('tvg-id', ''),
                                'tvg_name': ch.get('tvg-name', ''),
                                'logo': ch.get('tvg-logo', ''),
                                'user_agent': ch.get('user_agent', ''),
                                'program_id': ch.get('program-id', '')
                            })
                    self.m3u_channels_list = normalized_list

                if not self.m3u_channels_list:
                    return (False, "No valid channels found in M3U file")

                # Create JSON structure
                json_data = {"playlist": []}
                total_channels = len(self.m3u_channels_list)
                for idx, channel in enumerate(self.m3u_channels_list):
                    if self.cancel_conversion:
                        return (False, "Conversion cancelled")

                    channel_name = channel.get('name', 'Unknown')
                    # Update progress
                    progress = (idx + 1) / total_channels * 100
                    self.update_progress(
                        idx + 1,
                        _("Converting: {name} ({percent}%)").format(
                            name=channel_name,
                            percent=int(progress)
                        )
                    )

                    # Copy all normalized attributes
                    channel_data = {key: value for key, value in channel.items()}
                    json_data["playlist"].append(channel_data)

                # Generate output filename
                base_name = basename(self.selected_file).split('.')[0]
                output_dir = dirname(self.selected_file)
                output_file = join(output_dir, f"{base_name}.json")

                # Write JSON file
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=4, ensure_ascii=False)

                return (True, output_file, len(json_data["playlist"]))

            except Exception as e:
                return (False, str(e))

        # Reset UI and start conversion
        self.reset_conversion_buttons()
        self.is_converting = True
        self.cancel_conversion = False
        self["key_red"].setText("")
        self["key_green"].setText("")
        self["key_blue"].setText(_("Cancel"))

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def _convert_json_to_tv(self):
        """Convert JSON to TV bouquet format."""
        if self.is_converting:
            self.session.open(MessageBox, _("Conversion already in progress"), MessageBox.TYPE_WARNING)
            return

        # Global cleanup before conversion
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.epg_mapper._cleanup_all_match_types()
            self.epg_mapper._cleanup_log_file()

        # If the channel list hasn't been loaded yet, parse it from the selected file
        if not self.m3u_channels_list:
            self._parse_json_file(self.selected_file)

        # If still empty, show error
        if not self.m3u_channels_list:
            self.session.open(MessageBox, _("No valid channels found in the JSON file"), MessageBox.TYPE_ERROR)
            return

        def conversion_task():
            """Task per il thread"""
            try:
                # Check cancellation immediately
                if self.cancel_conversion:
                    return (False, "Conversion cancelled before start")
                if hasattr(self, 'epg_mapper') and self.epg_mapper:
                    self.epg_mapper._refresh_config()
                return self.core_converter.safe_conversion(self._real_conversion_task, self.selected_file, None)
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"Conversion task failed: {str(e)}")
                return (False, str(e))

        # Reset UI and start conversion
        self.reset_conversion_buttons()

        self.is_converting = True
        self.cancel_conversion = False
        self["key_red"].setText("")
        self["key_green"].setText("")
        self["key_blue"].setText(_("Cancel"))

        # Start memory cleanup timer
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.epg_mapper.optimize_memory_timer.start(30000)

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def _convert_xspf_to_m3u(self):
        """Convert XSPF to M3U format."""
        if self.is_converting:
            self.session.open(MessageBox, _("Conversion already in progress"), MessageBox.TYPE_WARNING)
            return

        # Global cleanup before conversion
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.epg_mapper._cleanup_all_match_types()
            self.epg_mapper._cleanup_log_file()

        def _xspf_conversion():
            try:
                tree = ET.parse(self.selected_file)
                root = tree.getroot()
                output_file = self.get_output_filename()
                ns = {'ns': 'http://xspf.org/ns/0/'}

                tracks = root.findall('.//ns:track', ns)
                total_items = len(tracks)

                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write('#EXTM3U\n')
                    f.write('#EXTENC: UTF-8\n')
                    f.write(f'#EXTARCHIMEDE: Generated by Archimede Converter {CURRENT_VERSION}\n')

                    for track in root.findall('.//ns:track', ns):
                        title = track.find('ns:title', ns)
                        location = track.find('ns:location', ns)

                        if title is not None and location is not None:
                            if self.cancel_conversion:
                                return (False, "Conversion cancelled")

                            f.write(f'#EXTINF:-1,{title.text}\n')
                            f.write(f'{location.text}\n')

                            if track % 10 == 0:
                                progress = (track + 1) / total_items * 100
                                self.update_progress(
                                    track + 1,
                                    _("Converting: {title} ({percent}%)").format(
                                        title=title,
                                        percent=int(progress)
                                    )
                                )

                track_count = len(tracks)

                return (True, output_file, track_count)
            except ET.ParseError:
                return (False, _("Invalid XSPF file"))
            except Exception as e:
                return (False, _("XSPF conversion error: %s") % str(e))

        def conversion_task():
            try:
                if self.cancel_conversion:
                    return (False, "Conversion cancelled before start")
                if hasattr(self, 'epg_mapper') and self.epg_mapper:
                    self.epg_mapper._refresh_config()
                return _xspf_conversion()
            except Exception as e:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error(f"Conversion task failed: {str(e)}")
                return (False, str(e))

        # Reset UI and start conversion
        self.reset_conversion_buttons()

        self.is_converting = True
        self.cancel_conversion = False
        self["key_red"].setText("")
        self["key_green"].setText("")
        self["key_blue"].setText(_("Cancel"))

        # Start memory cleanup timer
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.epg_mapper.optimize_memory_timer.start(30000)

        threads.deferToThread(conversion_task).addBoth(self.conversion_finished)

    def _open_editor_delayed(self):
        """Opens the manual editor after a short delay."""
        try:
            if hasattr(self, 'open_editor_timer'):
                self.open_editor_timer.stop()

            if not hasattr(self, 'm3u_channels_list') or not self.m3u_channels_list:
                logger.error("❌ No channel data available for the editor")
                return

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("🎯 Opening ManualMatchEditor...")

            # Use the bouquet name from the selected file
            bouquet_name = ""
            if hasattr(self, 'selected_file') and self.selected_file:
                bouquet_name = basename(self.selected_file).split('.')[0]

            def editor_closed_callback(result=None):
                """Callback executed when the manual editor is closed."""
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("🔒 ManualMatchEditor closed")

                # Show updated statistics after editing
                if hasattr(self, 'last_conversion_stats'):
                    updated_stats = self.calculate_updated_stats()
                    self.show_conversion_stats(self.conversion_type, updated_stats)

                # Automatically reload services if configured
                _reload_services_after_delay()

                self["status"].setText(_("Manual editing completed"))

            # Open the editor
            self.session.openWithCallback(
                editor_closed_callback,
                ManualMatchEditor,
                self.m3u_channels_list,
                self.epg_mapper,
                bouquet_name
            )

        except Exception as e:
            logger.error(f"❌ Error opening manual editor: {str(e)}")
            if hasattr(self, 'last_conversion_stats'):
                self.show_conversion_stats(self.conversion_type, self.last_conversion_stats)

    def _editor_closed_callback(self, result=None):
        """Callback when the manual editor is closed"""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info("🔒 ManualMatchEditor closed")

        # Show updated statistics after editing
        if hasattr(self, 'last_conversion_stats'):
            updated_stats = self.calculate_updated_stats()
            self.show_conversion_stats(self.conversion_type, updated_stats)

        # Auto-reload services if configured
        _reload_services_after_delay()

        self["status"].setText(_("Manual editing completed"))

    def conversion_finished(self, result):
        """Handles the completion of conversion"""
        try:
            # Reset UI immediately
            self["progress_source"].setValue(0)
            self["progress_text"].setText("")
            self.is_converting = False
            self.cancel_conversion = False

            # Reset UI buttons
            self._reset_conversion_ui()

            if not isinstance(result, tuple):
                logger.error("❌ Invalid conversion result format")
                self["status"].setText(_("Conversion failed: Invalid result"))
                return

            success = result[0]

            if success:
                # Stop memory optimization if active
                if hasattr(self, 'epg_mapper') and self.epg_mapper:
                    self.epg_mapper.optimize_memory_timer.stop()

                # PRESERVE STATISTICS BEFORE ANY RESET
                self.preserve_conversion_stats()

                # Collect conversion statistics only for TV conversion
                if self.conversion_type in ["m3u_to_tv", "json_to_tv", "tv_to_tv"]:
                    if hasattr(self, 'epg_mapper') and self.epg_mapper:
                        cache_stats = self.epg_mapper._get_cache_statistics()

                        total_processed = cache_stats.get('total_matches', 0)
                        effective_coverage = cache_stats.get('effective_coverage', 0)

                        stats_data = {
                            'total_channels': total_processed,
                            'effective_epg_matches': int(total_processed * effective_coverage / 100),
                            'effective_coverage': f"{effective_coverage:.1f}%",
                            'rytec_matches': cache_stats.get('rytec_matches', 0),
                            'dvb_matches': cache_stats.get('dvb_matches', 0),
                            'dvbt_matches': cache_stats.get('dvbt_matches', 0),
                            'fallback_matches': cache_stats.get('fallback_matches', 0),
                            'manual_db_matches': cache_stats.get('manual_db_matches', 0),
                        }

                        self.last_conversion_stats = stats_data
                        self.last_cache_stats = cache_stats
                        self.last_conversion_success = True

                # AUTO-OPEN EDITOR LOGIC only for TV conversion
                if (config.plugins.m3uconverter.auto_open_editor.value and
                        self.conversion_type in ["m3u_to_tv", "json_to_tv", "tv_to_tv"] and
                        hasattr(self, 'm3u_channels_list') and self.m3u_channels_list):

                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info("🎯 AUTO-OPENING MANUAL EDITOR...")

                    # Small delay to stabilize the UI
                    self.open_editor_timer = eTimer()
                    self.open_editor_timer.callback.append(self._open_editor_delayed)
                    self.open_editor_timer.start(1000, True)  # 1 secondo, single shot

                else:
                    # Show normal statistics for TV conversion
                    if self.conversion_type in ["m3u_to_tv", "json_to_tv", "tv_to_tv"]:
                        if hasattr(self, 'last_conversion_stats'):
                            self.show_conversion_stats(self.conversion_type, self.last_conversion_stats)
                        else:
                            self.show_normal_conversion_success()
                    else:
                        # For other conversions, just show normal success
                        self.show_normal_conversion_success()

                # Auto-reload services for TV conversion
                if self.conversion_type in ["m3u_to_tv", "json_to_tv", "tv_to_tv"]:
                    _reload_services_after_delay()

            else:
                # Gestione errore
                error_msg = result[1] if len(result) > 1 else _("Unknown error")
                logger.error(f"❌ Conversion failed: {error_msg}")
                self.session.open(
                    MessageBox,
                    _("Conversion failed: {}").format(error_msg),
                    MessageBox.TYPE_ERROR,
                    timeout=10
                )

            # Final state
            self["status"].setText(_("Conversion completed"))
            self["key_yellow"].setText(_("Match Editor"))
            self.file_loaded = False  # Reset file state

        except Exception as e:
            logger.error(f"❌ Error in conversion_finished: {str(e)}")
            import traceback
            logger.error(f"❌ Traceback: {traceback.format_exc()}")
            self.session.open(
                MessageBox,
                _("Error processing conversion result"),
                MessageBox.TYPE_ERROR
            )
            self._reset_conversion_ui()

    def show_normal_conversion_success(self):
        """Show success message for normal conversion"""
        try:
            if hasattr(self, 'selected_file') and self.selected_file:
                bouquet_name = self.get_safe_filename(basename(self.selected_file))
                safe_name = self.get_safe_filename(bouquet_name)
                display_name = self.remove_suffixes(safe_name)
            else:
                display_name = "Conversion"

            timestamp = strftime("%H:%M:%S")
            message = _(
                "✅ CONVERSION COMPLETED SUCCESSFULLY\n\n"
                "📁 Bouquet: {}\n"
                "🕒 Completed at: {}\n"
                "📍 Location: /etc/enigma2/userbouquet.*.tv\n\n"
                "Press OK to continue"
            ).format(display_name, timestamp)
            self.session.open(MessageBox, message, MessageBox.TYPE_INFO, timeout=6)

        except Exception as e:
            logger.error(f"Error showing success message: {str(e)}")

    def preserve_conversion_stats(self):
        """Preserve the current conversion statistics for later viewing"""
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            self.last_cache_stats = self.epg_mapper._get_cache_statistics()
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"💾 Statistics preserved: {self.last_cache_stats.get('total_matches', 0)} total matches")

    def show_conversion_stats(self, conversion_type, stats_data):
        """Show ACCURATE conversion statistics."""
        stats_message = [_("🎯 CONVERSION COMPLETE"), ""]
        try:
            timestamp = stats_data.get('timestamp', '')
            if not timestamp:
                timestamp = strftime("%Y-%m-%d %H:%M:%S")

            conversion_type_mapping = {
                "m3u_to_tv": "M3U to TV Channels",
                "json_to_tv": "JSON to TV Channels",
                "tv_to_m3u": "TV Channels to M3U",
                "json_to_m3u": "JSON to M3U",
                "xspf_to_m3u": "XSPF to M3U",
                "m3u_to_json": "M3U to JSON",
                "tv_to_json": "TV Channels to JSON"
            }

            # Use mapping, if not found use the original value
            display_conversion_type = conversion_type_mapping.get(
                conversion_type,
                conversion_type.replace('_', ' ').title()
            )

            # CORRECT COUNTERS - usa get() per accesso sicuro
            total_processed = stats_data.get('total_channels', 0)
            total_original = stats_data.get('total_original_channels', total_processed)
            effective_epg_matches = stats_data.get('effective_epg_matches', 0)
            effective_coverage = stats_data.get('effective_coverage', '0%')

            stats_message.append(_("📊 Total channels in file: {}").format(total_original))
            stats_message.append(_("✅ Valid channels processed: {}").format(total_processed))

            if total_original != total_processed:
                stats_message.append(_("🚫 Skipped channels: {}").format(total_original - total_processed))

            stats_message.append(_("🎯 Effective EPG matches: {}").format(effective_epg_matches))
            stats_message.append(_("📈 EFFECTIVE EPG coverage: {}").format(effective_coverage))

            # Add edit information if available
            if stats_data.get('status') == 'edited':
                stats_message.append(_("✏️ Manual edits applied: {}").format(stats_data.get('manual_matches', 0)))
                stats_message.append(_("🕒 Edited at: {}").format(stats_data.get('edit_timestamp', '')))
                stats_message.append("")

            # Add database mode info
            db_mode = stats_data.get('database_mode', 'both')
            mode_display = {
                "both": _("DVB + Rytec"),
                "dvb": _("Only DVB"),
                "rytec": _("Only Rytec"),
                "full": _("DVB + Rytec + DTT (Full)"),
                "dtt": _("Only DTT")
            }
            stats_message.append(_("🗄️ Database mode: {}").format(mode_display.get(db_mode, db_mode)))
            stats_message.append("")

            # Conversion-type specific statistics
            if conversion_type in ["m3u_to_tv", "json_to_tv"]:
                rytec_matches = stats_data.get('rytec_matches', 0)
                dvb_matches = stats_data.get('dvb_matches', 0)
                dvbt_matches = stats_data.get('dvbt_matches', 0)
                fallback_matches = stats_data.get('fallback_matches', 0)

                # Show matches based on database mode
                if db_mode == "full":
                    stats_message.extend([
                        _("🛰️ Rytec EPG matches: {}").format(rytec_matches),
                        _("📺 DVB EPG matches: {}").format(dvb_matches),
                        _("📡 DVB-T EPG matches: {}").format(dvbt_matches),
                        _("🔌 Fallback (no EPG): {}").format(fallback_matches)
                    ])
                elif db_mode == "both":
                    stats_message.extend([
                        _("🛰️ Rytec EPG matches: {}").format(rytec_matches),
                        _("📺 DVB EPG matches: {}").format(dvb_matches),
                        _("🔌 Fallback (no EPG): {}").format(fallback_matches)
                    ])
                elif db_mode == "dvb":
                    stats_message.extend([
                        _("📺 DVB EPG matches: {}").format(dvb_matches),
                        _("🔌 Fallback (no EPG): {}").format(fallback_matches)
                    ])
                elif db_mode == "rytec":
                    stats_message.extend([
                        _("🛰️ Rytec EPG matches: {}").format(rytec_matches),
                        _("🔌 Fallback (no EPG): {}").format(fallback_matches)
                    ])
                elif db_mode == "dtt":
                    stats_message.extend([
                        _("📡 DVB-T EPG matches: {}").format(dvbt_matches),
                        _("🔌 Fallback (no EPG): {}").format(fallback_matches)
                    ])

                stats_message.extend([
                    _("🎯 Effective EPG matches: {}").format(effective_epg_matches),
                    _("📈 EFFECTIVE EPG coverage: {}").format(effective_coverage),
                    _("   (Based on selected database mode: {})").format(mode_display.get(db_mode, db_mode))
                ])

                # Cache statistics if available
                cache_stats = stats_data.get('cache_stats', {})
                if cache_stats:
                    cache_stats = stats_data.get('cache_stats', {})
                    match_hit_rate = cache_stats.get('match_hit_rate', cache_stats.get('match_hit_rate', 'N/A'))
                    match_cache_size = cache_stats.get('match_cache_size', cache_stats.get('match_cache_size', 0))
                    rytec_channels = cache_stats.get('rytec_channels', 0)
                    dvb_channels = cache_stats.get('loaded_dvb_channels', 0)
                    stats_message.extend([
                        "",
                        _("💾 Cache efficiency: {}").format(match_hit_rate),
                        _("🔍 Cache size: {} entries").format(match_cache_size),
                        _("🗄️ Rytec channels in DB: {}").format(rytec_channels),
                        _("📡 DVB channels in DB: {}").format(dvb_channels)
                    ])

            elif conversion_type in ["tv_to_m3u", "json_to_m3u", "xspf_to_m3u", "m3u_to_json"]:
                output_file = stats_data.get('output_file', '')
                file_size = stats_data.get('file_size', 0)

                stats_message.extend([
                    _("💾 Output file: {}").format(output_file),
                    _("📁 File size: {}").format(self._format_file_size(file_size))
                ])

            # Add manual matches information
            manual_db_matches = stats_data.get('manual_db_matches', 0)
            if manual_db_matches > 0:
                stats_message.extend([
                    "",
                    _("💾 Manual DB matches: {}").format(manual_db_matches),
                    _("   (Reused previous manual corrections)")
                ])

            # FINAL INFORMATION WITH TIMESTAMP
            stats_message.extend([
                "",
                _("⏱️ Conversion type: {}").format(display_conversion_type),
                _("🕒 Completed at: {}").format(timestamp),
                _("✅ Status: Completed successfully")
            ])

        except Exception as e:
            logger.error(f"Error generating stats message: {str(e)}")
            stats_message.append(_("❌ Error generating statistics"))

        self.session.open(
            MessageBox,
            "\n".join(stats_message),
            MessageBox.TYPE_INFO,
            timeout=15
        )

    def _cancel_conversion_process(self):
        """Cancel the ongoing conversion"""
        if self.is_converting:
            self.cancel_conversion = True
            self.is_converting = False  # IMMEDIATELY stop conversion state
            self["key_blue"].setText(_("Cancelling..."))
            self["key_green"].setText(_("Stopped"))
            self["status"].setText(_("Cancelling conversion..."))
            logger.info("🛑 Conversion cancellation requested - forcing immediate stop")

            # Force UI update
            self["progress_source"].setValue(0)
            self["progress_text"].setText("")

            # Show immediate feedback
            self.session.open(MessageBox, _("Conversion cancelled"), MessageBox.TYPE_INFO, timeout=6)

            # Reset UI state
            self._reset_conversion_ui()

        else:
            # If not converting, show tools menu
            self._show_enhanced_tools_menu()

    def _reset_conversion_ui(self):
        """Completely reset conversion UI state"""
        self.is_converting = False
        self.cancel_conversion = False
        self["key_red"].setText(_("Open File"))
        self["key_green"].setText(_("Convert"))
        self["key_yellow"].setText("")
        self["key_blue"].setText(_("Tools"))
        self["progress_source"].setValue(0)
        self["progress_text"].setText("")
        self["status"].setText(_("Ready"))

    def _conversion_cancelled(self):
        """Handle conversion cancellation."""
        self.is_converting = False
        self.cancel_conversion = False
        self["key_red"].setText(_("Open File"))
        self["key_green"].setText(_("Convert"))
        self["key_blue"].setText(_("Tools"))
        self.session.open(MessageBox, _("Conversion cancelled"), MessageBox.TYPE_INFO, timeout=6)

    def _conversion_error(self, error_msg):
        """Handle conversion error."""
        self.is_converting = False
        self.cancel_conversion = False
        self["key_red"].setText(_("Open File"))
        self["key_green"].setText(_("Convert"))
        self["key_blue"].setText(_("Tools"))
        self.session.open(MessageBox, _("Conversion error: %s") % error_msg, MessageBox.TYPE_ERROR, timeout=6)

    def update_progress(self, value, text):
        """Update the progress bar safely."""
        try:
            callFromThread(self._update_progress_ui, value, text)
        except Exception as e:
            logger.error(f"Error updating progress: {str(e)}")

    def _update_progress_ui(self, value, text):
        """Update progress UI."""
        try:
            total_items = len(self.m3u_channels_list) if self.m3u_channels_list else 100
            self.progress_source.setRange(total_items)
            self.progress_source.setValue(value)
            self["progress_text"].setText(str(text))
        except Exception as e:
            logger.error(f"Error in UI progress update: {str(e)}")

    def start_player(self, name, url):
        """Start media player with specified channel."""
        try:
            if hasattr(self, 'initial_service') and self.initial_service:
                self.session.nav.stopService()

            stream = eServiceReference(4097, 0, url)
            stream.setName(name)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.log("DEBUG", f"Extracted video URL: {str(stream)}")
            self.session.nav.playService(stream)

            if hasattr(self, 'aspect_manager'):
                self.aspect_manager.set_aspect_for_video()

        except Exception as e:
            logger.error(f"Error starting player: {str(e)}")
            self["status"].setText(_("Playback error"))

    def _show_plugin_information(self):
        """Show plugin information - STAY IN TOOLS"""
        def info_closed(result=None):
            try:
                self._show_enhanced_tools_menu()
            except Exception as e:
                logger.error(f"Error returning to tools: {str(e)}")
                self["status"].setText(_("Ready"))

        try:
            self.session.openWithCallback(
                info_closed,
                PluginInfoScreen
            )
        except Exception as e:
            logger.error(f"Error opening plugin info: {str(e)}")
            self.session.open(
                MessageBox,
                _("Error opening plugin information: %s") % str(e),
                MessageBox.TYPE_ERROR
            )

    def show_info(self, message):
        """Show info message and log it."""
        logger.info(message)
        self.session.open(
            MessageBox,
            message,
            MessageBox.TYPE_INFO,
            timeout=6
        )
        self["status"].setText(message)

    def _show_error_message(self, message):
        """Show error message and log it."""
        logger.error(message)
        self.session.open(
            MessageBox,
            message,
            MessageBox.TYPE_ERROR,
            timeout=6
        )
        self["status"].setText(message)

    def open_editor_after_conversion(self):
        """Open the manual editor after conversion with the current data"""
        if not hasattr(self, 'm3u_channels_list') or not self.m3u_channels_list:
            self.session.open(MessageBox, _("No conversion data available. Please run a conversion first."), MessageBox.TYPE_WARNING)
            return

        bouquet_name = ""
        if hasattr(self, 'selected_file') and self.selected_file:
            bouquet_name = basename(self.selected_file).split('.')[0]

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"🎯 Opening editor for bouquet: {bouquet_name}")

        self.session.openWithCallback(
            self._editor_closed_callback,
            ManualMatchEditor,
            self.m3u_channels_list,
            self.epg_mapper,
            bouquet_name
        )

    def show_editor_statistics(self, result=None):
        """Show statistics after editor closes - then stay in UniversalConverter"""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug("🔒 UniversalConverter: Editor closed, showing statistics")

        if not self or not hasattr(self, 'session') or hasattr(self, '_showing_stats'):
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug("Statistics already showing or screen not available")
            return

        try:
            self._showing_stats = True
            updated_stats = self.calculate_updated_stats()

            self.safe_show_stats(updated_stats)

        except Exception as e:
            logger.error(f"Error showing editor statistics: {e}")
            self._showing_stats = False

    def safe_show_stats(self, stats_data):
        """Safely show statistics and stay in current screen"""
        try:
            if self and hasattr(self, 'session'):
                stats_message = self._prepare_stats_message(stats_data)

                self.session.openWithCallback(
                    self._stats_closed,
                    MessageBox,
                    stats_message,
                    MessageBox.TYPE_INFO,
                    timeout=15
                )
            else:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("Cannot show statistics - screen closed")
                self._showing_stats = False
        except Exception as e:
            logger.error(f"Error in safe_show_stats: {e}")
            self._showing_stats = False

    def _stats_closed(self, result=None):
        """Callback when statistics MessageBox is closed - stay in UniversalConverter"""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug("Statistics MessageBox closed - staying in UniversalConverter")
        self._showing_stats = False
        self["status"].setText(_("Ready for next operation"))
        self["key_red"].setText(_("Open File"))
        self["key_green"].setText(_("Convert"))
        self["key_yellow"].setText("")
        self["key_blue"].setText(_("Tools"))

    def _prepare_stats_message(self, stats_data):
        """Prepare complete statistics message"""
        try:
            total_channels = stats_data.get('total_channels', 0)
            epg_channels = stats_data.get('effective_epg_matches', 0)
            epg_percentage = stats_data.get('effective_coverage', '0%')
            manual_count = stats_data.get('manual_matches', 0)

            # Get all match types
            rytec_matches = stats_data.get('rytec_matches', 0)
            dvb_matches = stats_data.get('dvb_matches', 0)
            dvbt_matches = stats_data.get('dvbt_matches', 0)
            fallback_matches = stats_data.get('fallback_matches', 0)

            # Database mode info
            db_mode = stats_data.get('database_mode', 'both')
            mode_display = {
                "both": "DVB + Rytec",
                "dvb": "Only DVB",
                "rytec": "Only Rytec",
                "full": "DVB + Rytec + DTT",
                "dtt": "Only DTT"
            }

            message = [
                "🎯 CONVERSION STATISTICS",
                # "=" * 40,
                f"📊 Total channels processed: {total_channels}",
                f"📈 EPG coverage: {epg_channels}/{total_channels} ({epg_percentage})",
                "",
                "🔧 MATCH BREAKDOWN:",
                f"  🛰️ Rytec matches: {rytec_matches}",
                f"  📡 DVB-S matches: {dvb_matches}",
                f"  📺 DVB-T matches: {dvbt_matches}",
                f"  🔌 Fallback (no EPG): {fallback_matches}",
            ]

            if manual_count > 0:
                message.append(f"  ✏️  Manual corrections: {manual_count}")

            message.extend([
                "",
                "⚙️ CONFIGURATION:",
                f"  Database mode: {mode_display.get(db_mode, db_mode)}",
                f"  Bouquet mode: {config.plugins.m3uconverter.bouquet_mode.value}",
                f"  EPG generation: {config.plugins.m3uconverter.epg_generation_mode.value}",
            ])

            # Cache statistics if available
            cache_stats = stats_data.get('cache_stats', {})
            if cache_stats:
                match_hit_rate = cache_stats.get('match_hit_rate', cache_stats.get('match_hit_rate', 'N/A'))
                match_cache_size = cache_stats.get('match_cache_size', cache_stats.get('match_cache_size', 0))

                message.extend([
                    "",
                    "💾 PERFORMANCE:",
                    f"  Cache efficiency: {match_hit_rate}",
                    f"  Cache size: {match_cache_size} entries",
                ])

            # Editor information if available
            if stats_data.get('status') == 'edited':
                message.extend([
                    "",
                    "✏️ EDITOR INFO:",
                    f"  Edited at: {stats_data.get('edit_timestamp', '')}",
                ])

            message.extend([
                "",
                "⏱️ " + stats_data.get('timestamp', ''),
                "",
            ])

            return "\n".join(message)

        except Exception as e:
            logger.error(f"Error preparing stats message: {e}")
            return "Complete statistics summary available"

    def calculate_updated_stats(self):
        """Calculate complete updated statistics after editing"""
        if not hasattr(self, 'm3u_channels_list') or not self.m3u_channels_list:
            return self.last_conversion_stats

        # Count all match types
        rytec_matches = 0
        dvb_matches = 0
        dvbt_matches = 0
        manual_matches = 0
        fallback_matches = 0

        for channel in self.m3u_channels_list:
            match_type = channel.get('match_type', '')
            if 'manual_rytec' in match_type:
                rytec_matches += 1
                manual_matches += 1
            elif 'manual_dvb' in match_type:
                dvb_matches += 1
                manual_matches += 1
            elif 'manual_dvbt' in match_type:
                dvbt_matches += 1
                manual_matches += 1
            elif 'rytec' in match_type and 'manual' not in match_type:
                rytec_matches += 1
            elif 'dvb' in match_type and 'manual' not in match_type:
                dvb_matches += 1
            elif 'dvbt' in match_type:
                dvbt_matches += 1
            else:
                fallback_matches += 1

        total_channels = len(self.m3u_channels_list)

        # Calculate effective coverage based on database mode
        db_mode = self.epg_mapper.database_mode if hasattr(self, 'epg_mapper') and self.epg_mapper else "both"

        if db_mode == "full":
            effective_epg_matches = rytec_matches + dvb_matches + dvbt_matches
        elif db_mode == "both":
            effective_epg_matches = rytec_matches + dvb_matches
        elif db_mode == "dvb":
            effective_epg_matches = dvb_matches
        elif db_mode == "rytec":
            effective_epg_matches = rytec_matches
        elif db_mode == "dtt":
            effective_epg_matches = dvbt_matches
        else:
            effective_epg_matches = rytec_matches + dvb_matches

        effective_coverage = f"{(effective_epg_matches / total_channels * 100):.1f}%" if total_channels > 0 else "0%"

        # Update stats data
        updated_stats = self.last_conversion_stats.copy()
        updated_stats.update({
            'rytec_matches': rytec_matches,
            'dvb_matches': dvb_matches,
            'dvbt_matches': dvbt_matches,
            'fallback_matches': fallback_matches,
            'manual_matches': manual_matches,
            'effective_epg_matches': effective_epg_matches,
            'effective_coverage': effective_coverage,
            'status': 'edited',
            'edit_timestamp': strftime("%Y-%m-%d %H:%M:%S")
        })

        return updated_stats

    def print_detailed_conversion_stats(self):
        """Print accurate conversion statistics to logger"""
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            try:
                stats = self.epg_mapper._get_cache_statistics()

                # Get the REAL counts from statistics
                total_processed = stats.get('total_processed', 436)  # Use actual processed count
                rytec_matches = stats.get('rytec_matches', 0)
                dvb_matches = stats.get('dvb_matches', 0)
                dvbt_matches = stats.get('dvbt_matches', 0)
                fallback_matches = stats.get('fallback_matches', 0)
                manual_db_matches = stats.get('manual_db_matches', 0)

                # Calculate the REAL total (should match total_processed)
                real_total = rytec_matches + dvb_matches + dvbt_matches + fallback_matches + manual_db_matches

                logger.info("🔍 FINAL COUNT VERIFICATION:")
                logger.info(f"   Total valid channels: {total_processed}")
                logger.info(f"   Total calculated matches: {real_total}")
                logger.info(f"   Rytec: {rytec_matches}, DVB-S: {dvb_matches}, DVB-T: {dvbt_matches}")
                logger.info(f"   Manual DB: {manual_db_matches}, Fallback: {fallback_matches}")

                # Check consistency
                if real_total != total_processed:
                    logger.warning(f"⚠️ COUNT MISMATCH: Calculated {real_total} vs Processed {total_processed}")
                    # Auto-adjust to match reality
                    # scale_factor = total_processed / real_total if real_total > 0 else 1
                    adjusted_fallback = max(0, total_processed -
                                            (rytec_matches + dvb_matches + dvbt_matches + manual_db_matches))
                    logger.info(f"🔧 ADJUSTED: Fallback now {adjusted_fallback}, Total now {total_processed}")

                logger.info("🎯 ===== DETAILED CONVERSION STATISTICS =====")
                logger.info(f"📊 Total channels processed: {total_processed}")
                logger.info(f"🛰️ Rytec matches: {rytec_matches}")
                logger.info(f"📡 DVB-S matches: {dvb_matches}")
                logger.info(f"📺 DVB-T matches: {dvbt_matches}")
                logger.info(f"💾 Manual DB matches: {manual_db_matches}")
                logger.info(f"🔌 Fallback matches: {fallback_matches}")

                # Calculate percentages based on REAL total
                if total_processed > 0:
                    rytec_pct = (rytec_matches / total_processed) * 100
                    dvb_pct = (dvb_matches / total_processed) * 100
                    dvbt_pct = (dvbt_matches / total_processed) * 100
                    manual_pct = (manual_db_matches / total_processed) * 100
                    fallback_pct = (fallback_matches / total_processed) * 100
                else:
                    rytec_pct = dvb_pct = dvbt_pct = manual_pct = fallback_pct = 0

                logger.info("📈 MATCH PERCENTAGES:")
                logger.info(f"   Rytec: {rytec_pct:.1f}%")
                logger.info(f"   DVB-S: {dvb_pct:.1f}%")
                logger.info(f"   DVB-T: {dvbt_pct:.1f}%")
                logger.info(f"   Manual DB: {manual_pct:.1f}%")
                logger.info(f"   Fallback: {fallback_pct:.1f}%")

                # Cache statistics
                logger.info("💾 CACHE PERFORMANCE:")
                logger.info(f"   Hit rate: {stats.get('match_hit_rate', 'N/A')}")
                logger.info(f"   Cache size: {stats.get('match_cache_size', 0)} entries")

                logger.info("==========================================")

            except Exception as e:
                logger.error(f"Error printing detailed stats: {str(e)}")

    def print_simple_stats(self):
        """Print simple conversion statistics without cache details."""
        if hasattr(self, 'epg_mapper') and self.epg_mapper:
            try:
                return self.print_detailed_conversion_stats()
            except Exception as e:
                logger.error(f"Error printing simple stats: {str(e)}")


class ManualMatchEditor(Screen):
    """Manual EPG Match Editor - Edit ALL channels"""

    if SCREEN_WIDTH > 1280:
        skin = """
        <screen name="ManualMatchEditor" position="center,center" size="1920,1080" title="Manual EPG Match Editor" flags="wfNoBorder">
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1920,1080" zPosition="-2" />
            <widget source="Title" render="Label" position="64,13" size="1120,52" font="Regular; 32" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
            <!-- LEFT - CONVERTED CHANNELS -->
            <eLabel position="65,80" size="900,50" font="Regular;28" text="CONVERTED CHANNELS" transparent="0" halign="center" valign="center" />
            <widget name="channel_list" position="65,140" size="900,700" itemHeight="50" font="Regular;28" scrollbarMode="showOnDemand" />
            <!-- RIGHT - SUGGESTED MATCHES -->
            <eLabel position="1000,80" size="900,50" font="Regular;28" text="SUGGESTED MATCHES" transparent="0" halign="center" valign="center" />
            <widget name="match_list" position="1000,140" size="900,700" itemHeight="90" font="Regular;28" scrollbarMode="showOnDemand" />
            <!-- STATUS -->
            <widget name="status" position="65,860" size="1835,50" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
            <!-- KEYS -->
            <eLabel name="" position="1598,1018" size="52,52" backgroundColor="#002a2a2a" halign="center" valign="center" transparent="0" cornerRadius="26" font="Regular; 17" zPosition="1" text="CH+" />
            <eLabel name="" position="1658,1018" size="52,52" backgroundColor="#002a2a2a" halign="center" valign="center" transparent="0" cornerRadius="26" font="Regular; 17" zPosition="1" text="CH-" />
            <eLabel name="" position="1718,1018" size="52,52" backgroundColor="#002a2a2a" halign="center" valign="center" transparent="0" cornerRadius="26" font="Regular; 17" zPosition="1" text="OK" />
            <!--#####red####/-->
            <eLabel backgroundColor="#00ff0000" position="38,1050" size="375,9" zPosition="12" />
            <widget name="key_red" position="38,990" size="375,68" zPosition="11" font="Regular; 34" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
            <widget source="key_red" render="Label" position="38,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
            <!--#####green####/-->
            <eLabel backgroundColor="#0000ff00" position="420,1050" size="375,9" zPosition="12" />
            <widget name="key_green" position="420,990" size="375,68" zPosition="11" font="Regular;32" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
            <widget source="key_green" render="Label" position="420,990" size="375,68" zPosition="11" font="Regular;32" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
            <!--#####yellow####/-->
            <eLabel backgroundColor="#00ffff00" position="812,1050" size="375,9" zPosition="12" />
            <widget name="key_yellow" position="808,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
            <widget source="key_yellow" render="Label" position="808,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
            <!--#####blue####/-->
            <eLabel backgroundColor="#000000ff" position="1197,1050" size="375,9" zPosition="12" />
            <widget name="key_blue" position="1196,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,Button,Label" transparent="1" />
            <widget source="key_blue" render="Label" position="1196,990" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,StaticText" transparent="1" />
        </screen>"""
    else:
        skin = """
        <screen name="ManualMatchEditor" position="center,center" size="1280,720" title="Manual EPG Match Editor" flags="wfNoBorder">
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1280,720" zPosition="-2" />
            <widget source="Title" render="Label" position="25,8" size="1120,52" font="Regular;24" noWrap="1" transparent="1" valign="center" zPosition="1" halign="left" />
            <!-- LEFT - CONVERTED CHANNELS -->
            <eLabel position="25,60" size="600,30" font="Regular;22" text="CONVERTED CHANNELS" transparent="0" halign="center" valign="center" />
            <widget name="channel_list" position="25,100" size="600,450" itemHeight="45" font="Regular;24" scrollbarMode="showOnDemand" />
            <!-- RIGHT - SUGGESTED MATCHES -->
            <eLabel position="650,60" size="600,30" font="Regular;22" text="SUGGESTED MATCHES" transparent="0" halign="center" valign="center" />
            <widget name="match_list" position="650,100" size="600,450" itemHeight="90" font="Regular;24" scrollbarMode="showOnDemand" />
            <!-- STATUS -->
            <widget name="status" position="25,570" size="1230,40" font="Regular;22" backgroundColor="background" transparent="1" foregroundColor="white" />
            <!-- KEYS -->
            <eLabel name="" position="1062,654" size="52,52" backgroundColor="#002a2a2a" halign="center" valign="center" transparent="0" cornerRadius="26" font="Regular; 17" zPosition="1" text="CH+" />
            <eLabel name="" position="1120,654" size="52,52" backgroundColor="#002a2a2a" halign="center" valign="center" transparent="0" cornerRadius="26" font="Regular; 17" zPosition="1" text="CH-" />
            <eLabel name="" position="1181,655" size="52,52" backgroundColor="#002a2a2a" halign="center" valign="center" transparent="0" cornerRadius="26" font="Regular; 17" zPosition="1" text="OK" />
            <!--#####red####/-->
            <eLabel backgroundColor="#00ff0000" position="25,700" size="250,6" zPosition="12" />
            <widget name="key_red" position="25,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
            <widget source="key_red" render="Label" position="25,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
            <!--#####green####/-->
            <eLabel backgroundColor="#0000ff00" position="280,700" size="250,6" zPosition="12" />
            <widget name="key_green" position="280,660" size="250,45" zPosition="11" font="Regular;26" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
            <widget source="key_green" render="Label" position="280,660" size="250,45" zPosition="11" font="Regular;26" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
            <!--#####yellow####/-->
            <eLabel backgroundColor="#00ffff00" position="541,700" size="250,6" zPosition="12" />
            <widget name="key_yellow" position="539,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
            <widget source="key_yellow" render="Label" position="539,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
            <!--#####blue####/-->
            <eLabel backgroundColor="#000000ff" position="798,700" size="250,6" zPosition="12" />
            <widget name="key_blue" position="797,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,Button,Label" transparent="1" />
            <widget source="key_blue" render="Label" position="797,660" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,StaticText" transparent="1" />
        </screen>"""

    def __init__(self, session, conversion_data, epg_mapper, bouquet_name="", callback=None, parent_callback=None):
        Screen.__init__(self, session)
        self.session = session
        logger.info("✅ Manual database match editor initialized")
        self.manual_db = ManualDatabaseManager()
        self.core_converter = core_converter
        self.db_path = DB_PATCH

        self.conversion_data = conversion_data
        self.epg_mapper = epg_mapper
        # not used self.bouquet_name?
        self.bouquet_name = bouquet_name
        logger.info(f"   ManualMatchEditor bouquet_name: {bouquet_name}")
        self.callback = callback
        self.parent_callback = parent_callback

        self.current_channel_index = 0
        self.current_suggestions = []
        self.current_focus = "left"
        self.changes_made = False
        self.undo_stack = []
        self.max_undo_stack = 10

        self.setTitle(_("Manual EPG Match Editor"))
        self["channel_list"] = MenuList([])
        self["match_list"] = MenuList([])
        self["status"] = Label(_("Select a channel to correct the match"))
        self["key_red"] = StaticText(_("Close"))
        self["key_green"] = StaticText(_("Save All"))
        self["key_yellow"] = StaticText(_("Undo"))
        self["key_blue"] = StaticText(_("Reset"))
        self["actions"] = ActionMap(["ColorActions", "OkCancelActions", "DirectionActions", "ChannelSelectBaseActions"], {
            "red": self.request_close,
            "green": self.save_all_changes,
            "yellow": self.undo_last_action,
            "blue": self.reset_channel_match,
            "ok": self.ok,
            "cancel": self.request_close,
            "up": self.up,
            "down": self.down,
            # "left": self.focus_left,
            # "right": self.focus_right,
            "left": self.focus_changed,
            "right": self.focus_changed,
            "nextBouquet": self.page_down,
            "prevBouquet": self.page_up,
        }, -1)

        self.onLayoutFinish.append(self.start_editor)

    def start_editor(self):
        """Start the editor with improved EPG detection"""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug("🔍 ENHANCED DEBUG: Checking conversion_data EPG status:")

        epg_count = 0
        no_epg_count = 0
        manual_count = 0

        # for idx, channel in enumerate(self.conversion_data[:200]):
        for idx, channel in enumerate(self.conversion_data):
            name = channel.get('name', 'Unknown')
            match_type = channel.get('match_type', 'unknown')
            sref = channel.get('sref', '')
            tvg_id = channel.get('tvg_id', '')

            # Enhanced EPG detection
            has_epg = False
            epg_type = "NO EPG"

            if any(epg_type in str(match_type).lower() for epg_type in ['rytec', 'dvb', 'dvbt']):
                has_epg = True
                epg_type = "AUTO EPG"
            elif 'manual' in str(match_type).lower():
                has_epg = True
                epg_type = "MANUAL EPG"
                manual_count += 1
            elif sref and sref.startswith('1:0:'):  # DVB reference
                has_epg = True
                epg_type = "DVB EPG"

            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"   {idx}: {name[:30]} -> {match_type} | {epg_type} | TVG: {tvg_id}")

            if has_epg:
                epg_count += 1
            else:
                no_epg_count += 1

        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"📊 ENHANCED EPG Summary: {epg_count} with EPG, {no_epg_count} without EPG, {manual_count} manual")

        self.force_epg_detection()

        self.update_channel_list()
        if self.conversion_data:
            self.current_focus = "left"
            self["channel_list"].selectionEnabled(1)
            self["match_list"].selectionEnabled(0)
            self.channel_selected()

    def ok(self):
        """Handle OK button - assign match when in right list"""
        if self.current_focus == "right":
            self.assign_selected_match()
        else:
            # If in left list, move focus to right and select first item
            self.focus_right()
            # Auto-select first suggestion if available
            if self.current_suggestions:
                self["match_list"].moveToIndex(0)

    def force_epg_detection(self):
        """Force EPG detection for channels with match_type unknown."""
        enhanced_count = 0
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info("🔄 FORCING EPG search for channels with match_type unknown...")

        for channel in self.conversion_data:
            current_match_type = channel.get('match_type', 'unknown')

            # If match_type is unknown or iptv_fallback, attempt EPG search
            if current_match_type in ['unknown', 'iptv_fallback']:
                name = channel.get('name', '')
                tvg_id = channel.get('tvg_id', '')
                clean_name = self.epg_mapper.clean_channel_name(name)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"🔍 Forced EPG lookup for: {name}")
                    logger.debug(f"   TVG ID: {tvg_id}")
                    logger.debug(f"   Clean name: {clean_name}")

                # Force lookup with optimized parameters
                service_ref, match_type = self.epg_mapper._find_best_service_match(
                    clean_name,
                    tvg_id,
                    name,
                    channel.get('url', '')
                )
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug(f"   Lookup result: service_ref={service_ref}, match_type={match_type}")

                # If a DVB/Rytec match is found, update channel info
                if service_ref and service_ref.startswith('1:0:'):
                    channel['sref'] = service_ref
                    channel['match_type'] = match_type
                    channel['original_sref'] = service_ref  # Store for reset
                    enhanced_count += 1
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"✅ EPG FOUND for: {name} -> {match_type}")
                        logger.debug(f"   New service_ref: {service_ref}")
                        logger.debug(f"   New match_type: {match_type}")
                else:
                    logger.debug(f"❌ No EPG found for: {name}")

        if enhanced_count > 0:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"🎯 Forced EPG detected for {enhanced_count} channels")
        else:
            logger.warning("⚠️ No EPG found during forced detection!")

    def save_all_changes(self):
        """Save all manual changes - called by GREEN button"""
        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("💾 SAVE ALL CORRECTED: Starting with proper manual detection")

            # 1. Use the CORRECT method to check for manual changes
            actually_modified_count = self.count_truly_manual_changes()

            if actually_modified_count == 0:
                self["status"].setText(_("No MANUAL changes to save"))
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("ℹ️ No TRULY MANUAL changes to save")
                return True

            # 2. Save to manual database using CORRECTED method
            saved_count = self.save_manual_mappings_to_database_corrected()

            if saved_count > 0:
                # 3. RESET changes_made flag after successful save
                self.changes_made = False

                self["status"].setText(_("Saved {} MANUAL corrections").format(saved_count))
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"🎉 Success: {saved_count} TRULY MANUAL corrections saved")

                # 4. Reload services
                self.reload_services_after_manual_edit()

                return True
            else:
                self["status"].setText(_("No MANUAL corrections saved"))
                return False

        except Exception as e:
            logger.error(f"❌ Save all CORRECTED error: {str(e)}")
            self["status"].setText(_("Save error"))
            return False

    def count_truly_manual_changes(self):
        """Count only TRULY manual modifications"""
        actually_modified_count = 0

        for channel in self.conversion_data:
            current_match_type = channel.get('match_type', '')
            original_sref = channel.get('original_sref', '')
            current_sref = channel.get('sref', '')

            # ONLY count if:
            # 1. It's marked as manual
            # 2. It's different from original
            # 3. We have an original to compare to
            if (current_match_type.startswith('manual_') and
                    original_sref != current_sref and
                    original_sref != ''):
                actually_modified_count += 1

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"🔍 MANUAL COUNT: Found {actually_modified_count} truly manual changes")
        return actually_modified_count

    def save_manual_mappings_to_database_corrected(self):
        """Save ONLY truly manual modifications"""
        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("💾 CORRECTED SAVE: Saving only TRUE manual modifications")

            if not hasattr(self, 'manual_db') or not self.manual_db:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("❌ Manual database not available")
                return 0

            # 1. Clean database first
            self.cleanup_database_before_save()

            # 2. Find ONLY truly manual modifications
            saved_count = 0

            for channel in self.conversion_data:
                current_match_type = channel.get('match_type', '')
                channel_name = channel.get('name', 'Unknown')
                original_sref = channel.get('original_sref', '')
                current_sref = channel.get('sref', '')

                # STRICT CRITERIA for manual saves:
                if (current_match_type.startswith('manual_') and
                    original_sref != current_sref and
                        original_sref != '' and  # Must have original
                        not any(auto_type in current_match_type for auto_type in ['auto_saved', 'consistent_fallback'])):

                    # Simplify match_type
                    simple_match_type = "manual_epg"
                    if 'rytec' in current_match_type:
                        simple_match_type = "manual_rytec"
                    elif 'dvb' in current_match_type:
                        simple_match_type = "manual_dvb"
                    elif 'dvbt' in current_match_type:
                        simple_match_type = "manual_dvbt"

                    mapping_data = {
                        'channel_name': channel_name,
                        'original_name': channel.get('original_name', channel_name),
                        'clean_name': self.epg_mapper.clean_channel_name(channel_name),
                        'tvg_id': channel.get('tvg_id', ''),
                        'assigned_sref': current_sref,
                        'match_type': simple_match_type,
                        'similarity': 1.0,
                        'bouquet_source': getattr(self, 'bouquet_name', 'manual_edit'),
                        'created': strftime("%Y-%m-%d %H:%M:%S"),
                        'last_used': strftime("%Y-%m-%d %H:%M:%S"),
                        'manually_modified': True
                    }

                    if self.manual_db.save_manual_mapping(mapping_data):
                        saved_count += 1
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info(f"✅ SAVED MANUAL: {saved_count}. {channel_name}")

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"🎯 FINAL MANUAL SAVE: {saved_count} channels")
            return saved_count

        except Exception as e:
            logger.error(f"❌ Corrected manual save error: {str(e)}")
            return 0

    def cleanup_database_before_save(self):
        """Clean the database before saving new mappings"""
        try:
            # Load the existing database with error recovery
            data = self.manual_db.load_database()
            if not data or not isinstance(data, dict):
                data = self.manual_db._get_default_structure()

            old_count = len(data.get('mappings', []))

            # Create a list of clean_name values for current channels
            current_clean_names = set()
            for channel in self.conversion_data:
                clean_name = self.epg_mapper.clean_channel_name(channel.get('name', ''))
                if clean_name:  # Only add non-empty clean names
                    current_clean_names.add(clean_name)

            # Filter the database: keep only mappings for existing channels
            filtered_mappings = []
            for mapping in data.get('mappings', []):
                if isinstance(mapping, dict) and mapping.get('clean_name') in current_clean_names:
                    # Ensure the mapping has all required fields
                    if not mapping.get('assigned_sref'):
                        continue  # Skip invalid mappings
                    filtered_mappings.append(mapping)

            # Update the database
            data['mappings'] = filtered_mappings
            data['last_updated'] = strftime("%Y-%m-%d %H:%M:%S")

            # Save with error handling
            success = self.manual_db.save_database(data)

            if success:
                new_count = len(filtered_mappings)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"🧹 DATABASE CLEANED: {old_count} -> {new_count} mappings")
                return True
            else:
                logger.error("❌ Failed to save cleaned database")
                return False

        except Exception as e:
            logger.error(f"❌ Database cleanup error: {str(e)}")
            return False

    def reload_services_after_manual_edit(self):
        """Reload services after a manual edit"""
        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("🔄 Reloading services after manual edit...")

            def do_reload():
                try:
                    # Give the system more time to process the bouquet files
                    time.sleep(2)  # Wait 2 seconds before reload
                    db = eDVBDB.getInstance()
                    if db:
                        db.reloadServicelist()
                        db.reloadBouquets()
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info("✅ Services successfully reloaded after manual edit")
                    else:
                        logger.warning("⚠️ Could not get eDVBDB instance for reload")
                except Exception as e:
                    logger.error(f"❌ Error during service reload: {str(e)}")
                finally:
                    # IMPORTANT: Stop the timer to avoid loops
                    if hasattr(self, 'reload_timer'):
                        self.reload_timer.stop()

            # Use a ONE-SHOT timer with longer delay
            self.reload_timer = eTimer()
            self.reload_timer.callback.append(do_reload)
            self.reload_timer.start(5000, True)  # 5 seconds delay, True = single shot

        except Exception as e:
            logger.error(f"❌ Error setting up service reload: {str(e)}")

    def do_final_close(self):
        """Final close procedure"""
        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug("ManualMatchEditor: Final closure")
            self.close()
        except Exception as e:
            logger.error(f"Error in do_final_close: {str(e)}")
            self.close()

    def request_close(self):
        """Handle closing"""
        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("ManualMatchEditor: Simple close requested - returning to parent")
            self.close()
        except Exception as e:
            logger.error(f"Error in ManualMatchEditor request_close: {str(e)}")
            self.close()

    def ask_save_before_close(self):
        """Ask user if they want to save before closing"""
        message = _("You have unsaved manual changes.\n\nDo you want to save before closing?")

        def callback(result):
            try:
                if result is not None:
                    if result:
                        # User said YES to save - save and close
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info("💾 User chose to save changes before closing")
                        if self.save_all_changes():
                            if config.plugins.m3uconverter.enable_debug.value:
                                logger.info("✅ Changes saved successfully, now closing")
                            self.do_final_close()
                        else:
                            if config.plugins.m3uconverter.enable_debug.value:
                                logger.error("❌ Failed to save changes")
                            # Still close even if save fails
                            self.do_final_close()
                    else:
                        # User said NO - close without saving
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info("❌ User chose to close without saving")
                        self.do_final_close()
            except Exception as e:
                logger.error(f"❌ Error in save callback: {str(e)}")
                self.do_final_close()

        self.session.openWithCallback(
            callback,
            MessageBox,
            message,
            MessageBox.TYPE_YESNO,
            timeout=15,
            default=True  # Default to YES for safety
        )

    def update_channel_list(self):
        """Update the left channel list with accurate EPG status"""
        items = []
        epg_channels = 0
        no_epg_channels = 0

        for idx, channel in enumerate(self.conversion_data):
            name = channel.get('name', 'Unknown')
            match_type = channel.get('match_type', 'unknown')
            original_name = channel.get('original_name', name)
            sref = channel.get('sref', '')

            # Improved EPG detection logic
            has_epg = False
            status_text = ""
            epg_source = ""

            # 1. Check the match_type first
            if any(epg_type in str(match_type).lower() for epg_type in ['rytec', 'dvb', 'dvbt']):
                has_epg = True
                if 'rytec' in match_type.lower():
                    epg_source = "🛰️"
                    status_text = f" ({epg_source} RYTEC)"
                elif 'dvbt' in match_type.lower():
                    epg_source = "📺"
                    status_text = f" ({epg_source} DVB-T)"
                elif 'dvb' in match_type.lower():
                    epg_source = "📡"
                    status_text = f" ({epg_source} DVB)"
                else:
                    epg_source = "📡"
                    status_text = f" ({epg_source} EPG)"

            elif 'manual' in match_type.lower():
                has_epg = True
                epg_source = "✏️"
                status_text = f" ({epg_source} MANUAL)"

            # 2. If no match_type, check the service reference
            elif sref and sref.startswith('1:0:'):
                has_epg = True
                epg_source = "🔍"
                status_text = f" ({epg_source} DVB)"

            else:
                # 3. No EPG found
                status_text = " (❌ NO EPG)"

            # Main icon (EPG status)
            icon = "✅" if has_epg else "❌"

            # Service type icon (IPTV / DVB-S / DVB-T / Other)
            if sref.startswith('4097:'):
                service_icon = "🌐"
            elif sref.startswith('1:0:1:'):
                service_icon = "🛰️"
            elif sref.startswith('1:0:16:'):
                service_icon = "📺"
            elif sref.startswith('1:0:10:'):
                service_icon = "🔌"
            else:
                service_icon = "❓"

            # Count totals
            if has_epg:
                epg_channels += 1
            else:
                no_epg_channels += 1

            # Build display name (shortened for UI)
            display_name = f"{icon}{service_icon} {original_name[:35]}{status_text}"
            if len(display_name) > 55:
                display_name = display_name[:52] + "..."

            items.append(display_name)

        # Update UI list
        self["channel_list"].setList(items)

        # Update footer stats
        total_channels = len(self.conversion_data)
        epg_percentage = (epg_channels / total_channels * 100) if total_channels > 0 else 0
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"📊 FINAL STATS: {epg_channels} with EPG, {no_epg_channels} without EPG")

        self["status"].setText(_("EPG Coverage: {}/{} channels ({:.1f}%)").format(
            epg_channels, total_channels, epg_percentage))

    def channel_selected(self):
        """When selecting a channel on the left"""
        selected_index = self["channel_list"].getSelectedIndex()
        if selected_index < 0 or selected_index >= len(self.conversion_data):
            return

        self.current_channel_index = selected_index
        channel_data = self.conversion_data[selected_index]

        channel_name = channel_data.get('name', 'Unknown')
        match_type = channel_data.get('match_type', 'unknown')
        sref = channel_data.get('sref', 'None')
        tvg_id = channel_data.get('tvg_id', 'None')

        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"📺 Channel selected: {channel_name}")
            logger.debug(f"   Match type: {match_type}")
            logger.debug(f"   Service ref: {sref[:60]}")
            logger.debug(f"   TVG ID: {tvg_id}")

        # Enhanced status display
        has_epg = any(epg_type in str(match_type).lower() for epg_type in ['rytec', 'dvb', 'dvbt', 'manual']) or sref.startswith('1:0:')
        epg_status = "✅ HAS EPG" if has_epg else "❌ NO EPG"

        self["status"].setText(_("Channel: {} | {} | Match: {} | Press RIGHT for matches").format(
            channel_name, epg_status, match_type
        ))

        self.update_suggested_matches(selected_index)

    def update_suggested_matches(self, channel_index):
        """Update suggested matches on the right"""
        # Store current channel index for thread safety
        self.current_channel_index = channel_index

        # Clear previous results immediately
        self.current_suggestions = []
        self["match_list"].setList(["Loading suggestions..."])

        # Update status
        if 0 <= channel_index < len(self.conversion_data):
            channel_name = self.conversion_data[channel_index].get('name', 'Unknown')
            self["status"].setText(_("Searching matches for: {}...").format(channel_name))

        # Start background thread using twisted
        callInThread(self._find_matches_in_background, channel_index)

    def _find_matches_in_background(self, channel_index):
        """Background thread for finding matches - MODIFIED FOR MORE SUGGESTIONS"""
        # Check that we are still on the same channel
        if channel_index != self.current_channel_index:
            return

        channel_data = self.conversion_data[channel_index]
        channel_name = channel_data.get('name', '')
        tvg_id = channel_data.get('tvg_id', '')

        # IMMEDIATE GUI UPDATE - show that we are searching
        self._safe_update_ui([], show_loading=True)

        # --- PREPARE CLEAN NAMES ---
        # Remove quality indicators
        name_without_quality = self.epg_mapper._quality_pattern.sub('', channel_name).strip()

        # Compact version (all characters together)
        clean_name_no_quality = self.epg_mapper.clean_channel_name(name_without_quality)

        # Version with spaces (to search for "Cine 34" etc.)
        spaced_no_quality = ' '.join(name_without_quality.lower().split())

        # Full compact version (old behavior)
        clean_name = self.epg_mapper.clean_channel_name(channel_name)

        all_matches = []

        # 0. CASE-INSENSITIVE SEARCH (high priority)
        if channel_index == self.current_channel_index:
            case_insensitive_matches = self.epg_mapper._search_case_insensitive_matches(
                channel_name, clean_name_no_quality, tvg_id
            )
            if case_insensitive_matches:
                all_matches.extend(case_insensitive_matches)
                self._safe_update_ui(all_matches, show_loading=False)

        # 1. FAST SEARCH - Clean name without quality words (more important)
        if channel_index == self.current_channel_index:
            # Try both versions: compact and spaced
            rytec_matches = self.search_rytec_matches(channel_name, clean_name_no_quality, tvg_id)
            rytec_matches += self.search_rytec_matches(channel_name, spaced_no_quality, tvg_id)
            dvb_matches = self.search_dvb_matches(channel_name, clean_name_no_quality)
            dvb_matches += self.search_dvb_matches(channel_name, spaced_no_quality)
            batch_results = rytec_matches + dvb_matches
            if batch_results:
                all_matches.extend(batch_results)
                self._safe_update_ui(all_matches, show_loading=False)

        # 2. SEARCH WITH QUALITY WORDS (if necessary)
        if channel_index == self.current_channel_index and len(all_matches) < 15:  # INCREASED FROM 8 TO 15
            rytec_with_quality = self.search_rytec_matches(channel_name, clean_name, tvg_id)
            dvb_with_quality = self.search_dvb_matches(channel_name, clean_name)
            batch_results = rytec_with_quality + dvb_with_quality
            if batch_results:
                all_matches.extend(batch_results)
                self._safe_update_ui(all_matches, show_loading=False)

        # 3. IMPROVED: PRESERVE ORIGINAL NAME STRUCTURE FOR SHORT NAMES
        if channel_index == self.current_channel_index and len(all_matches) < 30:  # INCREASED FROM 10 TO 25
            # For short names or names with numbers (like "20 Mediaset"), use less aggressive cleaning
            original_words = channel_name.split()

            # Try variations of the original name with minimal cleaning
            if len(original_words) <= 3:  # Short names
                original_clean = ' '.join(original_words).lower().strip()
                if original_clean and len(original_clean) >= 3:
                    original_rytec = self.search_rytec_matches(channel_name, original_clean, tvg_id)
                    original_dvb = self.search_dvb_matches(channel_name, original_clean)
                    batch_results = original_rytec + original_dvb
                    if batch_results:
                        all_matches.extend(batch_results)
                        self._safe_update_ui(all_matches, show_loading=False)

            # Also try preserving numbers (like "20 mediaset")
            name_with_numbers = ' '.join(original_words).lower().strip()
            if name_with_numbers and name_with_numbers != clean_name_no_quality:
                numbers_rytec = self.search_rytec_matches(channel_name, name_with_numbers, tvg_id)
                numbers_dvb = self.search_dvb_matches(channel_name, name_with_numbers)
                batch_results = numbers_rytec + numbers_dvb
                if batch_results:
                    all_matches.extend(batch_results)
                    self._safe_update_ui(all_matches, show_loading=False)

        # 4. WORD COMBINATIONS (NO LENGTH LIMIT)
        if channel_index == self.current_channel_index and len(all_matches) < 40:  # INCREASED FROM 15 TO 35
            words = name_without_quality.split()
            if len(words) > 1:
                # All 2-word combinations (no length limit)
                for i in range(len(words) - 1):
                    if channel_index != self.current_channel_index:
                        break
                    phrase = f"{words[i]} {words[i + 1]}"
                    phrase_clean = self.epg_mapper.clean_channel_name(phrase)
                    if phrase_clean:
                        phrase_rytec = self.search_rytec_matches(channel_name, phrase_clean, '')[:3]  # INCREASED FROM 2 TO 3
                        phrase_dvb = self.search_dvb_matches(channel_name, phrase_clean)[:3]  # INCREASED FROM 2 TO 3
                        batch_results = phrase_rytec + phrase_dvb
                        if batch_results:
                            all_matches.extend(batch_results)
                            self._safe_update_ui(all_matches, show_loading=False)

        # 5. SINGLE WORDS (NO LENGTH LIMIT)
        if channel_index == self.current_channel_index:
            words = name_without_quality.split()
            for word in words:
                if channel_index != self.current_channel_index:
                    break
                # NO LENGTH CHECK - search all words
                word_clean = self.epg_mapper.clean_channel_name(word)
                if word_clean:
                    word_rytec = self.search_rytec_matches(channel_name, word_clean, '')[:3]  # INCREASED FROM 2 TO 3
                    word_dvb = self.search_dvb_matches(channel_name, word_clean)[:3]  # INCREASED FROM 2 TO 3
                    batch_results = word_rytec + word_dvb
                    if batch_results:
                        all_matches.extend(batch_results)
                        self._safe_update_ui(all_matches, show_loading=False)

        # 6. SEARCH IN RYTEC IDS FOR KEYWORDS (new - no length limit)
        if channel_index == self.current_channel_index:
            words = name_without_quality.split()
            for word in words:
                if channel_index != self.current_channel_index:
                    break
                word_clean = self.epg_mapper.clean_channel_name(word)
                if word_clean:
                    # Search in Rytec IDs and names
                    id_matches = self.epg_mapper._find_rytec_ids_by_keyword(word_clean)
                    if id_matches:
                        all_matches.extend(id_matches)
                        self._safe_update_ui(all_matches, show_loading=False)

        # 7. SEARCH MEANINGFUL WORDS (FILTERED - BETTER QUALITY)
        if channel_index == self.current_channel_index and len(all_matches) < 50:  # INCREASED FROM 20 TO 40
            words = name_without_quality.split()
            meaningful_words = []

            # Filter meaningful words (not quality, at least 2 characters)
            for word in words:
                word_clean = self.epg_mapper.clean_channel_name(word)
                if word_clean and len(word_clean) >= 2:
                    # Check that it is not a quality-related word
                    if not self.epg_mapper._quality_pattern.search(word):
                        meaningful_words.append(word_clean)

            for word in meaningful_words:
                if channel_index != self.current_channel_index:
                    break
                id_matches = self.epg_mapper._find_rytec_ids_by_keyword(word)
                if id_matches:
                    all_matches.extend(id_matches)
                    self._safe_update_ui(all_matches, show_loading=False)

        # 8. DIRECT SEARCH IN RYTEC FOR CLEAN NAME
        if channel_index == self.current_channel_index and len(all_matches) < 60:  # INCREASED FROM 25 TO 50
            # Search directly in Rytec database using clean name
            clean_for_rytec = clean_name_no_quality.replace(' ', '').lower()

            # Search exact or partial matches in Rytec database
            for rytec_id, service_ref in self.epg_mapper.mapping.rytec['basic'].items():
                if not service_ref:
                    continue

                rytec_clean = rytec_id.lower().replace('.it', '')

                # If the clean name is contained in Rytec ID or vice versa
                if (clean_for_rytec in rytec_clean or rytec_clean in clean_for_rytec) and len(clean_for_rytec) >= 3:
                    similarity = self.epg_mapper._calculate_similarity(clean_for_rytec, rytec_clean)
                    if similarity > 0.4:  # Low threshold for partial matches
                        all_matches.append({
                            'type': 'rytec',
                            'sref': service_ref,
                            'name': f"Rytec: {rytec_id}",
                            'similarity': similarity,
                            'priority': 90
                        })

            if all_matches:
                self._safe_update_ui(all_matches, show_loading=False)

        # 9. SPECIFIC SEARCH FOR "NUMBER NAME" PATTERN (e.g., "20 Mediaset" -> "20Mediaset")
        if channel_index == self.current_channel_index and len(all_matches) < 70:  # INCREASED FROM 30 TO 60
            words = name_without_quality.split()

            # Search pattern like "20 Mediaset" → "20Mediaset"
            if len(words) == 2:
                word1, word2 = words

                # If one word is numeric and the other textual
                if (word1.isdigit() and not word2.isdigit()) or (word2.isdigit() and not word1.isdigit()):
                    if word1.isdigit():
                        number, name = word1, word2
                    else:
                        number, name = word2, word1

                    # Generate possible combinations
                    combinations = [
                        f"{number}{name}",           # "20Mediaset"
                        f"{name}{number}",           # "Mediaset20"
                        f"{number}{name}.it",        # "20Mediaset.it"
                        f"{name}{number}.it",        # "Mediaset20.it"
                        f"{number}.{name}.it",       # "20.Mediaset.it"
                        f"{name}.{number}.it",       # "Mediaset.20.it"
                    ]

                    # Search each combination in Rytec database
                    for combo in combinations:
                        if combo in self.epg_mapper.mapping.rytec['basic']:
                            service_ref = self.epg_mapper.mapping.rytec['basic'][combo]
                            if service_ref and self.epg_mapper._is_service_compatible(service_ref):
                                all_matches.append({
                                    'type': 'rytec',
                                    'sref': service_ref,
                                    'name': f"Rytec: {combo}",
                                    'similarity': 0.7,
                                    'priority': 90
                                })

                    # Also search in DVB database
                    for combo in combinations:
                        clean_combo = self.epg_mapper.clean_channel_name(combo)
                        if clean_combo in self.epg_mapper.mapping.dvb:
                            for service in self.epg_mapper.mapping.dvb[clean_combo]:
                                service_type = 'dvbt' if self.epg_mapper._is_dvb_t_service(service['sref']) else 'dvb'
                                all_matches.append({
                                    'type': service_type,
                                    'sref': service['sref'],
                                    'name': f"{service_type.upper()}: {combo}",
                                    'similarity': 0.7,
                                    'priority': 90
                                })

            if all_matches:
                self._safe_update_ui(all_matches, show_loading=False)

        # 10. IMPROVED SEARCH FOR SHORT NAMES AND NUMBERED CHANNELS
        if channel_index == self.current_channel_index and len(all_matches) < 70:  # INCREASED FROM 35 TO 70
            short_name_matches = self.epg_mapper._enhanced_search_short_names(clean_name_no_quality, channel_name)
            if short_name_matches:
                all_matches.extend(short_name_matches)
                self._safe_update_ui(all_matches, show_loading=False)

        # Final update - show completion
        if channel_index == self.current_channel_index:
            self._safe_update_ui(all_matches, show_loading=False, is_final=True)

    def _safe_update_ui(self, all_matches, show_loading=False, is_final=False):
        """Thread-safe UI update with current matches"""
        if not hasattr(self, 'current_channel_index'):
            return

        # Remove duplicates
        seen_srefs = set()
        unique_matches = []
        for match in all_matches:
            if match['sref'] not in seen_srefs:
                unique_matches.append(match)
                seen_srefs.add(match['sref'])

        # Sort and limit - INCREASED LIMIT FROM 25 TO 50
        unique_matches.sort(key=lambda x: (x.get('priority', 0), x['similarity']), reverse=True)
        final_matches = unique_matches[:50]  # INCREASED FROM 25 TO 50
        self.current_suggestions = final_matches

        # Build match list for UI with service reference (as in old code)
        match_items = []

        if show_loading:
            match_items.append("🔄 Searching for matches...")
        elif not final_matches and is_final:
            match_items.append("❌ No matches found")
        elif not final_matches:
            match_items.append("🔍 Still searching...")
        else:
            for i, match in enumerate(final_matches):
                similarity_pct = int(match['similarity'] * 100)
                icons = {'rytec': '🛰️', 'dvb': '📡', 'dvbt': '📺'}
                icon = icons.get(match['type'], '❓')

                # Show service reference as in old code
                sref_short = match['sref'][:30] + "..." if len(match['sref']) > 30 else match['sref']
                match_text = f"{i + 1}. {icon} {similarity_pct}%\n{match['name'][:35]}\n{sref_short}"
                match_items.append(match_text)

        # Update UI in main thread using callFromThread
        callFromThread(self._update_match_list, match_items, len(final_matches), is_final)

    def _update_match_list(self, match_items, match_count, is_final=False):
        """Update match list in main thread"""
        try:
            if self["match_list"] and hasattr(self, 'current_channel_index'):
                self["match_list"].setList(match_items)

                # Update status with match count
                if 0 <= self.current_channel_index < len(self.conversion_data):
                    channel_name = self.conversion_data[self.current_channel_index].get('name', 'Unknown')
                    if is_final:
                        if match_count > 0:
                            self["status"].setText(_("Found {} matches for: {} | Select and press OK").format(match_count, channel_name))
                        else:
                            self["status"].setText(_("No matches found for: {}").format(channel_name))
                    else:
                        if match_count > 0:
                            self["status"].setText(_("Found {} matches so far for: {} | Searching...").format(match_count, channel_name))
                        else:
                            self["status"].setText(_("Searching matches for: {}...").format(channel_name))

        except Exception as e:
            print(f"Error updating match list: {e}")

    def search_rytec_matches(self, channel_name, clean_name, tvg_id):
        """Search matches in Rytec database"""
        matches = []

        # FIX: Add safety check for epg_mapper
        if not hasattr(self, 'epg_mapper') or not self.epg_mapper:
            logger.error("❌ epg_mapper not available in search_rytec_matches")
            return matches

        # Search by exact TVG ID first (highest priority)
        if tvg_id and tvg_id.lower() != 'none':
            rytec_id = self.epg_mapper._convert_to_rytec_format(tvg_id)
            if rytec_id in self.epg_mapper.mapping.rytec['basic']:
                service_ref = self.epg_mapper.mapping.rytec['basic'][rytec_id]
                if service_ref and self.epg_mapper._is_service_compatible(service_ref):
                    matches.append({
                        'type': 'rytec',
                        'sref': service_ref,
                        'name': f"Rytec: {rytec_id}",
                        'similarity': 1.0,
                        'priority': 100
                    })

        # Search by name similarity in Rytec database
        limit = config.plugins.m3uconverter.rytec_search_limit.value
        for rytec_id, service_ref in list(self.epg_mapper.mapping.rytec['basic'].items())[:limit]:
            if not service_ref:
                continue

            # Calculate similarity with the clean name
            similarity = self.epg_mapper._calculate_similarity(clean_name, rytec_id.lower())

            # FIX: Add safety check before accessing similarity_threshold_rytec
            if hasattr(self.epg_mapper, 'similarity_threshold_rytec'):
                threshold = self.epg_mapper.similarity_threshold_rytec
            else:
                threshold = 0.7  # Default fallback

            if similarity > threshold:
                matches.append({
                    'type': 'rytec',
                    'sref': service_ref,
                    'name': f"Rytec: {rytec_id}",
                    'similarity': similarity,
                    'priority': 90
                })

        return matches

    def search_dvb_matches(self, channel_name, clean_name):
        """Search matches in DVB database"""
        matches = []

        # Search by exact clean name match
        if clean_name in self.epg_mapper.mapping.dvb:
            for service in self.epg_mapper.mapping.dvb[clean_name]:
                service_name = service.get('name', 'DVB Service')
                similarity = self.epg_mapper._calculate_similarity(clean_name, service_name)

                if similarity > self.epg_mapper.similarity_threshold_dvb:
                    service_type = 'dvbt' if self.epg_mapper._is_dvb_t_service(service['sref']) else 'dvb'

                    matches.append({
                        'type': service_type,
                        'sref': service['sref'],
                        'name': f"{service_type.upper()}: {service_name}",
                        'similarity': similarity,
                        'priority': 85 if service_type == 'dvbt' else 80
                    })

        # Extended search by similarity across all DVB services
        # for service_name, services in list(self.epg_mapper.mapping.dvb.items())[:1000]:  # Limit for performance
        limit = config.plugins.m3uconverter.dvb_search_limit.value
        for service_name, services in list(self.epg_mapper.mapping.dvb.items())[:limit]:
            if len(services) == 0:
                continue

            similarity = self.epg_mapper._calculate_similarity(clean_name, service_name)
            if similarity > self.epg_mapper.similarity_threshold_dvb:
                service = services[0]
                service_type = 'dvbt' if self.epg_mapper._is_dvb_t_service(service['sref']) else 'dvb'

                matches.append({
                    'type': service_type,
                    'sref': service['sref'],
                    'name': f"{service_type.upper()}: {service_name}",
                    'similarity': similarity,
                    'priority': 75 if service_type == 'dvbt' else 70
                })

        return matches

    def assign_selected_match(self):
        """Assign the selected match to current channel with OK button"""
        if self.current_focus != "right":
            self["status"].setText(_("First select a match from the right list (use RIGHT arrow)"))
            return

        match_index = self["match_list"].getSelectedIndex()
        if (match_index < 0 or match_index >= len(self.current_suggestions) or
                self.current_channel_index >= len(self.conversion_data)):
            self["status"].setText(_("Please select a valid match from the right list"))
            return

        channel_data = self.conversion_data[self.current_channel_index]
        selected_match = self.current_suggestions[match_index]

        # DEFINE channel_name
        channel_name = channel_data.get('name', 'Unknown')

        # Save current state for undo
        self.save_undo_state(channel_data)

        # Store original data for reset capability - IMPORTANT FIX
        if 'original_sref' not in channel_data:
            channel_data['original_sref'] = channel_data.get('sref', '')
            channel_data['original_match_type'] = channel_data.get('match_type', '')

        # Match type - mark as TRULY MANUAL
        base_type = selected_match['type']  # 'rytec', 'dvb', 'dvbt'
        match_type = f"manual_{base_type}"  # 'manual_rytec', 'manual_dvb', 'manual_dvbt'

        channel_data['sref'] = selected_match['sref']
        channel_data['match_type'] = match_type
        channel_data['assigned_match'] = selected_match['name']
        channel_data['manually_modified'] = True  # Explicit flag

        # CORRECTED: MARK CHANGES MADE only if TRULY different from original
        original_sref = channel_data.get('original_sref', '')
        current_sref = channel_data.get('sref', '')

        if original_sref and original_sref != current_sref:
            self.changes_made = True
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"📝 MANUAL CHANGE: {channel_name} - {original_sref[:30]} -> {current_sref[:30]}")
        else:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug(f"ℹ️ No real change: {channel_name}")

        # Refresh display
        self.update_channel_list()
        self["channel_list"].moveToIndex(self.current_channel_index)

        # Return focus to left list AUTOMATICALLY
        self.current_focus = "left"
        self["channel_list"].selectionEnabled(1)
        self["match_list"].selectionEnabled(0)
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Manual assignment CONFIRMED: {channel_name} -> {match_type}")

        self["status"].setText(_("EPG assigned: {} | {}").format(
            channel_name, selected_match['name']))

        # Refresh suggestions for next channel
        # self.channel_selected() # better on break, after EPG assignment, suggested refreshments are useless

    def save_undo_state(self, channel_data):
        """Save current state for undo functionality"""
        try:
            undo_state = {
                'channel_index': self.current_channel_index,
                'channel_name': channel_data.get('name', ''),
                'previous_sref': channel_data.get('sref', ''),
                'previous_match_type': channel_data.get('match_type', ''),
                'timestamp': strftime("%H:%M:%S")
            }

            # Add to undo stack
            self.undo_stack.append(undo_state)

            # Limit stack size
            if len(self.undo_stack) > self.max_undo_stack:
                self.undo_stack.pop(0)

        except Exception as e:
            logger.error(f"Error saving undo state: {str(e)}")

    def undo_last_action(self):
        """Undo the last manual assignment"""
        if not self.undo_stack:
            self["status"].setText(_("No actions to undo"))
            return

        try:
            last_action = self.undo_stack.pop()
            channel_index = last_action['channel_index']

            if 0 <= channel_index < len(self.conversion_data):
                channel_data = self.conversion_data[channel_index]

                # Restore previous state
                channel_data['sref'] = last_action['previous_sref']
                channel_data['match_type'] = last_action['previous_match_type']

                # Remove assigned match info
                if 'assigned_match' in channel_data:
                    del channel_data['assigned_match']

                # Update display
                self.update_channel_list()
                self["channel_list"].moveToIndex(channel_index)
                self.current_channel_index = channel_index

                self["status"].setText(_("Undo: Restored previous state for {}").format(
                    last_action['channel_name']))

                # Refresh suggestions
                self.channel_selected()
            else:
                self["status"].setText(_("Error: Channel index out of range"))

        except Exception as e:
            logger.error(f"Error undoing action: {str(e)}")
            self["status"].setText(_("Error undoing action"))

    def reset_channel_match(self):
        """Reset the match for the current channel to its original state"""
        if self.current_channel_index >= len(self.conversion_data):
            return

        channel_data = self.conversion_data[self.current_channel_index]
        channel_name = channel_data.get('name', 'Unknown')

        # Store current state for comparison
        current_sref = channel_data.get('sref', '')

        # CORRECTED: Only mark as change if there's a REAL difference
        if 'original_sref' in channel_data and current_sref != channel_data['original_sref']:
            self.changes_made = True
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"📝 RESET MARKED AS MANUAL CHANGE: {channel_name}")

        # Restore original data if available
        if 'original_sref' in channel_data:
            channel_data['sref'] = channel_data['original_sref']
            channel_data['match_type'] = channel_data.get('original_match_type', 'iptv_fallback')
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"🔄 RESET TO ORIGINAL: {channel_name}")
        else:
            # Regenerate IPTV reference as fallback
            url = channel_data.get('url', '')
            if url:
                channel_data['sref'] = self.epg_mapper._generate_service_reference(url)
                channel_data['match_type'] = 'iptv_fallback'
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"🔄 RESET TO IPTV: {channel_name}")

        # Remove assigned match info and manual flag
        if 'assigned_match' in channel_data:
            del channel_data['assigned_match']

        if 'manually_modified' in channel_data:
            del channel_data['manually_modified']

        self.update_channel_list()
        self["channel_list"].moveToIndex(self.current_channel_index)
        self["status"].setText(_("Match reset for: {}").format(channel_name))

        # Refresh suggestions
        self.channel_selected()

    def write_group_bouquet(self, group, channels):
        """Use CoreConverter for bouquet writing"""
        safe_name = self.core_converter.get_safe_filename(group)
        return self.core_converter.write_group_bouquet(safe_name, channels, self.epg_mapper)

    def update_main_bouquet(self, groups):
        """Use CoreConverter for main bouquet update"""
        return self.core_converter.update_main_bouquet(groups)

    def get_safe_filename(self, name):
        """Use CoreConverter for filename generation"""
        return self.core_converter.get_safe_filename(name)

    def remove_suffixes(self, name):
        """Use CoreConverter for suffix removal"""
        return self.core_converter.remove_suffixes(name)

    def focus_changed(self):
        """Change focus between left and right lists"""
        if self.current_focus == "left":
            self.focus_right()
        else:
            self.focus_left()

    def focus_left(self):
        """Move focus to left list"""
        self.current_focus = "left"
        self["channel_list"].selectionEnabled(1)
        self["match_list"].selectionEnabled(0)
        self["status"].setText(_("Select a channel to edit (use OK or RIGHT to go to matches)"))

    def focus_right(self):
        """Move focus to right list"""
        if not self.conversion_data:
            self["status"].setText(_("No channels available"))
            return

        if self.current_channel_index >= len(self.conversion_data):
            self["status"].setText(_("Please select a channel first"))
            return

        self.current_focus = "right"
        self["channel_list"].selectionEnabled(0)
        self["match_list"].selectionEnabled(1)

        # Ensure we have suggestions
        if not self.current_suggestions:
            self.update_suggested_matches(self.current_channel_index)

        # Auto-select first suggestion if available
        if self.current_suggestions:
            self["match_list"].moveToIndex(0)
            self["status"].setText(_("Select a match and press OK to assign"))
        else:
            self["status"].setText(_("No matches found - try different search"))

    def up(self):
        """Navigate up in current list"""
        if self.current_focus == "left":
            self["channel_list"].up()
            self.channel_selected()
        else:
            self["match_list"].up()

    def down(self):
        """Navigate down in current list"""
        if self.current_focus == "left":
            self["channel_list"].down()
            self.channel_selected()
        else:
            self["match_list"].down()

    def page_up(self):
        if self.current_focus == "left":
            current_index = self["channel_list"].getSelectedIndex()
            new_index = max(0, current_index - 10)
            self["channel_list"].moveToIndex(new_index)
            self.channel_selected()
        else:
            current_index = self["match_list"].getSelectedIndex()
            new_index = max(0, current_index - 10)
            self["match_list"].moveToIndex(new_index)

    def page_down(self):
        if self.current_focus == "left":
            current_index = self["channel_list"].getSelectedIndex()
            max_index = len(self.conversion_data) - 1
            new_index = min(max_index, current_index + 10)
            self["channel_list"].moveToIndex(new_index)
            self.channel_selected()
        else:
            current_index = self["match_list"].getSelectedIndex()
            max_index = len(self.current_suggestions) - 1
            new_index = min(max_index, current_index + 10)
            self["match_list"].moveToIndex(new_index)


class ManualDatabaseEditor(Screen):
    """Visual editor for manual mappings database with duplicate selection system"""

    if SCREEN_WIDTH > 1280:
        skin = """
        <screen name="ManualDatabaseEditor" position="center,center" size="1600,950" title="Manual Database Editor" flags="wfNoBorder">
            <eLabel backgroundColor="#002d3d5b" cornerRadius="20" position="0,0" size="1600,1000" zPosition="-2" />
            <widget source="Title" render="Label" position="50,20" size="1500,50" font="Regular; 32" noWrap="1" transparent="1" valign="center" zPosition="1" halign="center" />
            <!-- MAPPINGS LIST -->
            <eLabel position="50,80" size="1500,40" font="Regular;28" text="MANUAL MAPPINGS DATABASE" transparent="0" halign="center" valign="center" />
            <widget name="mapping_list" position="50,125" size="1500,700" itemHeight="40" font="Regular;28" scrollbarMode="showOnDemand" />
            <!-- STATUS -->
            <widget name="status" position="50,835" size="1500,40" font="Regular;28" backgroundColor="background" transparent="1" foregroundColor="white" />
            <!-- KEYS -->
            <!--#####red####/-->
            <eLabel backgroundColor="#00ff0000" position="38,950" size="375,9" zPosition="12" />
            <widget name="key_red" position="38,880" size="375,68" zPosition="11" font="Regular; 34" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
            <widget source="key_red" render="Label" position="38,880" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
            <!--#####green####/-->
            <eLabel backgroundColor="#0000ff00" position="420,950" size="375,9" zPosition="12" />
            <widget name="key_green" position="420,880" size="375,68" zPosition="11" font="Regular;32" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
            <widget source="key_green" render="Label" position="420,880" size="375,68" zPosition="11" font="Regular;32" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
            <!--#####yellow####/-->
            <eLabel backgroundColor="#00ffff00" position="812,950" size="375,9" zPosition="12" />
            <widget name="key_yellow" position="808,880" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
            <widget source="key_yellow" render="Label" position="808,880" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
            <!--#####blue####/-->
            <eLabel backgroundColor="#000000ff" position="1197,950" size="375,9" zPosition="12" />
            <widget name="key_blue" position="1196,880" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,Button,Label" transparent="1" />
            <widget source="key_blue" render="Label" position="1196,880" size="375,68" zPosition="11" font="Regular;32" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,StaticText" transparent="1" />
        </screen>"""
    else:
        skin = """
        <screen name="ManualDatabaseEditor" position="center,center" size="1200,700" title="Manual Database Editor" flags="wfNoBorder">
            <eLabel backgroundColor="#002d3d5b" cornerRadius="15" position="0,0" size="1200,700" zPosition="-2" />
            <widget source="Title" render="Label" position="30,15" size="1140,40" font="Regular;24" noWrap="1" transparent="1" valign="center" zPosition="1" halign="center" />
            <!-- MAPPINGS LIST -->
            <eLabel position="30,65" size="1140,30" font="Regular;22" text="MANUAL MAPPINGS DATABASE" transparent="0" halign="center" valign="center" />
            <widget name="mapping_list" position="30,105" size="1140,500" itemHeight="35" font="Regular;24" scrollbarMode="showOnDemand" />
            <!-- STATUS -->
            <widget name="status" position="30,610" size="1140,30" font="Regular;22" backgroundColor="background" transparent="1" foregroundColor="white" />
            <!-- KEYS -->
            <!--#####red####/-->
            <eLabel backgroundColor="#00ff0000" position="25,690" size="250,6" zPosition="12" />
            <widget name="key_red" position="25,645" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,Button,Label" transparent="1" />
            <widget source="key_red" render="Label" position="25,645" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_red,StaticText" transparent="1" />
            <!--#####green####/-->
            <eLabel backgroundColor="#0000ff00" position="280,690" size="250,6" zPosition="12" />
            <widget name="key_green" position="280,645" size="250,45" zPosition="11" font="Regular;26" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,Button,Label" transparent="1" />
            <widget source="key_green" render="Label" position="280,645" size="250,45" zPosition="11" font="Regular;26" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_green,StaticText" transparent="1" />
            <!--#####yellow####/-->
            <eLabel backgroundColor="#00ffff00" position="541,690" size="250,6" zPosition="12" />
            <widget name="key_yellow" position="539,645" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,Button,Label" transparent="1" />
            <widget source="key_yellow" render="Label" position="539,645" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_yellow,StaticText" transparent="1" />
            <!--#####blue####/-->
            <eLabel backgroundColor="#000000ff" position="798,690" size="250,6" zPosition="12" />
            <widget name="key_blue" position="797,645" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,Button,Label" transparent="1" />
            <widget source="key_blue" render="Label" position="797,645" size="250,45" zPosition="11" font="Regular;26" noWrap="1" valign="center" halign="center" backgroundColor="#05000603" objectTypes="key_blue,StaticText" transparent="1" />
        </screen>"""

    def __init__(self, session, epg_mapper=None):
        Screen.__init__(self, session)
        self.session = session
        self.epg_mapper = epg_mapper
        self.manual_db = ManualDatabaseManager()
        logger.info("✅ Manual database editor initialized")

        # Selection system
        self.selection_mode = False
        self.selected_items = set()
        self.mappings = []
        self.duplicates_data = []
        self.showing_duplicates = False
        self.changes_made = False

        self.setTitle(_("Manual Database Editor"))

        self["mapping_list"] = MenuList([], enableWrapAround=True)
        self["status"] = Label(_("Loading database..."))
        self["key_red"] = StaticText(_("Close"))
        self["key_green"] = StaticText(_("Select"))
        self["key_yellow"] = StaticText(_("Delete"))
        self["key_blue"] = StaticText(_("Duplicates"))

        self["actions"] = ActionMap(["ColorActions", "OkCancelActions"], {
            "red": self.request_close,
            "green": self.toggle_selection_mode,
            "yellow": self.delete_selected,
            "blue": self.toggle_duplicates_view,
            "ok": self.toggle_item_selection,
            "cancel": self.handle_cancel,
            # "cancel": self.request_close,
            # "green": self.edit_mapping,
            # "yellow": self.delete_mapping,
            # "blue": self.handle_blue_button,
            # "ok": self.edit_mapping,
        }, -1)

        self.onLayoutFinish.append(self.load_database)

    def load_database(self):
        """Reload the database while preserving view and selection state"""
        try:
            # Save the current state
            current_view_was_duplicates = self.showing_duplicates
            current_selection_mode = self.selection_mode  # 🔥 Save selection state

            # Force reload from file
            data = self.manual_db.load_database()
            self.mappings = data.get("mappings", [])

            # Clear selection but KEEP the mode
            self.selected_items.clear()
            self.selection_mode = current_selection_mode  # 🔥 Restore selection mode

            # Restore previous view
            if current_view_was_duplicates:
                duplicates = self.find_duplicates()
                if duplicates:
                    self.show_duplicates()
                else:
                    self.showing_duplicates = False
                    self.show_all_mappings()
                    self["status"].setText(_("No duplicates found"))
                    self.selection_mode = False  # Disable selection if no duplicates remain
            else:
                self.showing_duplicates = False
                self.show_all_mappings()

            # 🔥 Update button labels based on selection mode
            if self.selection_mode:
                self["key_green"].setText(_("Done"))
                self["status"].setText(_("Selection mode active. Select items with OK"))
            else:
                self["key_green"].setText(_("Select"))

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"✅ Database reloaded. Selection mode: {self.selection_mode}, View: {'duplicates' if self.showing_duplicates else 'all'}")

        except Exception as e:
            logger.error(f"Error loading database: {str(e)}")
            self["status"].setText(_("Error loading database"))

    def handle_cancel(self):
        """Handle CANCEL key - improved navigation"""
        if self.selection_mode:
            # If we are in selection mode, exit it
            self.exit_selection_mode()
        elif self.showing_duplicates:
            # If we are in duplicates view, return to normal view
            self.show_all_mappings()
        else:
            # Otherwise, close the editor
            self.request_close()

    def enter_selection_mode(self):
        """Enter selection mode"""
        # Make sure there are items to select
        if self.showing_duplicates:
            duplicates = self.find_duplicates()
            if not duplicates:
                self.session.open(
                    MessageBox,
                    _("No duplicates available for selection"),
                    MessageBox.TYPE_INFO,
                    timeout=3
                )
                return

        self.selection_mode = True
        self["key_green"].setText(_("Done"))
        self["status"].setText(_("Selection mode: Select items with OK"))

        # Refresh current view to show checkboxes

        self.refresh_current_view()

    def exit_selection_mode(self):
        """Exit selection mode"""
        self.selection_mode = False
        self.selected_items.clear()
        self["key_green"].setText(_("Select"))
        self["status"].setText(_("Selection mode off"))

        # Refresh current view per nascondere i checkbox
        self.refresh_current_view()

    def refresh_current_view(self):
        """Refresh the current view based on what's being shown"""
        if self.showing_duplicates:
            self.show_duplicates()
        else:
            self.show_all_mappings()

    def show_all_mappings(self):
        """Show all mappings in normal view"""
        display_list = []
        self.duplicates_data = []
        self.showing_duplicates = False

        if not self.mappings:
            display_list.append(_("No mappings available"))
        else:
            for i, mapping in enumerate(self.mappings):
                channel_name = mapping.get('channel_name', 'Unknown')
                match_type = mapping.get('match_type', 'unknown')
                # sref = mapping.get('assigned_sref', '')

                # Format for display
                prefix = "[X] " if i in self.selected_items else "[ ] " if self.selection_mode else ""
                # sref_short = sref[:30] + "..." if len(sref) > 30 else sref

                display_text = f"{prefix}{i + 1:03d}. {channel_name} [{match_type}]"
                if len(display_text) > 80:
                    display_text = display_text[:77] + "..."

                display_list.append(display_text)

        self["mapping_list"].setList(display_list)
        self.update_status()
        self["key_blue"].setText(_("Duplicates"))

    def find_duplicates(self):
        """Find duplicate mappings based on clean_name"""
        duplicates = {}

        for i, mapping in enumerate(self.mappings):
            clean_name = mapping.get('clean_name', '').lower().strip()
            if clean_name:
                if clean_name not in duplicates:
                    duplicates[clean_name] = []
                duplicates[clean_name].append((i, mapping))

        return {name: entries for name, entries in duplicates.items() if len(entries) > 1}

    def toggle_duplicates_view(self):
        """Toggle between all mappings and duplicates view - IMPROVED"""
        if not self.showing_duplicates:
            # Enter duplicates view
            duplicates = self.find_duplicates()
            if duplicates:
                self.show_duplicates()
            else:
                self.session.open(
                    MessageBox,
                    _("No duplicates found in the database"),
                    MessageBox.TYPE_INFO,
                    timeout=3
                )
        else:
            # Return to the normal (all mappings) view
            self.show_all_mappings()

    def toggle_selection_mode(self):
        """Toggle selection mode on or off"""
        if self.selection_mode:
            # If already in selection mode, exit it
            self.exit_selection_mode()
        else:
            # Otherwise, enter selection mode
            self.enter_selection_mode()

    def toggle_item_selection(self):
        """Toggle selection of current item"""
        if not self.selection_mode:
            return

        selected_index = self["mapping_list"].getSelectedIndex()

        if self.showing_duplicates:
            # Handle selection in duplicates view
            if selected_index < len(self.duplicates_data):
                entry_type = self.duplicates_data[selected_index][0]
                if entry_type == "duplicate":
                    orig_index = self.duplicates_data[selected_index][1]
                    if orig_index in self.selected_items:
                        self.selected_items.remove(orig_index)
                    else:
                        self.selected_items.add(orig_index)
                    self.show_duplicates()
        else:
            # Handle selection in normal view
            if 0 <= selected_index < len(self.mappings):
                if selected_index in self.selected_items:
                    self.selected_items.remove(selected_index)
                else:
                    self.selected_items.add(selected_index)
                self.show_all_mappings()

    def show_duplicates(self):
        """Display only duplicate mappings with selection support"""
        duplicates = self.find_duplicates()
        self.duplicates_data = []
        display_list = []

        if not duplicates:
            # If no duplicates, automatically return to the normal view
            self.showing_duplicates = False
            self.show_all_mappings()
            self["status"].setText(_("No duplicates found"))
            return

        self.showing_duplicates = True
        total_duplicates = sum(len(entries) for entries in duplicates.values())

        # Header with information
        if self.selection_mode:
            display_list.append(_(">>> SELECTION MODE - Select items with OK"))
            display_list.append(_(">>> {} duplicate groups, {} total mappings").format(len(duplicates), total_duplicates))
        else:
            display_list.append(_(">>> DUPLICATES VIEW"))
            display_list.append(_(">>> {} groups, {} mappings total").format(len(duplicates), total_duplicates))

        self.duplicates_data.append(("header", "info"))
        self.duplicates_data.append(("header", "info"))

        for clean_name, entries in duplicates.items():
            # Group header
            group_header = _("--- {} ({} duplicates) ---").format(clean_name, len(entries))
            display_list.append(group_header)
            self.duplicates_data.append(("group_header", clean_name))

            for orig_index, mapping in entries:
                if orig_index < len(self.mappings):
                    channel_name = mapping.get('channel_name', 'Unknown')
                    match_type = mapping.get('match_type', 'unknown')
                    sref = mapping.get('assigned_sref', '')

                    # Always show checkboxes when in selection mode
                    prefix = "[X] " if orig_index in self.selected_items else "[ ] " if self.selection_mode else ""

                    # Format display text
                    display_text = f"{prefix}{channel_name} [{match_type}]"
                    if sref:
                        sref_short = sref[:25] + "..." if len(sref) > 25 else sref
                        display_text += f" - {sref_short}"

                    if len(display_text) > 80:
                        display_text = display_text[:77] + "..."

                    display_list.append(display_text)
                    self.duplicates_data.append(("duplicate", orig_index, mapping))

        self["mapping_list"].setList(display_list)
        self.update_status()
        self["key_blue"].setText(_("All Mappings"))

    def update_status(self):
        """Update status bar with detailed selection info - IMPROVED"""
        selected_count = len(self.selected_items)

        if self.selection_mode:
            status_text = _("SELECTION MODE: {} items selected").format(selected_count)
            if self.showing_duplicates:
                duplicates = self.find_duplicates()
                if duplicates:
                    status_text += _(" - {} duplicate groups").format(len(duplicates))
        else:
            if self.showing_duplicates:
                duplicates = self.find_duplicates()
                if duplicates:
                    status_text = _("DUPLICATES: {} groups, {} mappings").format(
                        len(duplicates), sum(len(entries) for entries in duplicates.values()))
                else:
                    status_text = _("No duplicates found")
            else:
                status_text = _("ALL MAPPINGS: {} total").format(len(self.mappings))

        self["status"].setText(status_text)

    def delete_selected(self):
        """Delete selected mappings and remain in selection mode"""
        if not self.selected_items:
            self.session.open(
                MessageBox,
                _("No items selected for deletion"),
                MessageBox.TYPE_INFO,
                timeout=3
            )
            return

        selected_count = len(self.selected_items)
        message = _("Delete {} selected items?\n\nThis action cannot be undone.").format(selected_count)

        def confirm_callback(result):
            if result:
                # Save current state BEFORE deletion
                was_showing_duplicates = self.showing_duplicates

                # Perform deletion
                success = self.perform_bulk_delete()

                if success:
                    # Clear selection but REMAIN in selection mode
                    self.selected_items.clear()

                    # Reload the database
                    self.load_database()

                    # IMPORTANT: Stay in selection mode after deletion
                    self.selection_mode = True
                    self["key_green"].setText(_("Done"))

                    # Decide which view to show after deletion
                    if was_showing_duplicates:
                        remaining_duplicates = self.find_duplicates()
                        if remaining_duplicates:
                            # Still duplicates remaining, stay in duplicates view
                            self.show_duplicates()
                            self["status"].setText(_("Deleted {} items. Still in selection mode.").format(selected_count))
                        else:
                            # No more duplicates, return to normal view but REMAIN in selection mode
                            self.showing_duplicates = False
                            self.show_all_mappings()
                            self["status"].setText(_("Deleted {} items. No more duplicates.").format(selected_count))
                    else:
                        # Was in normal view, remain there
                        self.show_all_mappings()
                        self["status"].setText(_("Deleted {} items").format(selected_count))

                    logger.info(f"✅ Deleted {selected_count} items, remained in selection mode")

                else:
                    self["status"].setText(_("Error deleting items"))

        self.session.openWithCallback(confirm_callback, MessageBox, message, MessageBox.TYPE_YESNO)

    def perform_bulk_delete(self):
        """Actually delete the selected mappings"""
        try:
            # Load fresh data from file
            data = self.manual_db.load_database()
            current_mappings = data.get('mappings', [])

            if not current_mappings:
                return False

            # Create a new list without the selected items
            new_mappings = []
            deleted_indices = sorted(self.selected_items, reverse=True)

            # Create a copy of the current mappings
            temp_mappings = current_mappings.copy()

            # Remove selected items (from the end to avoid index shifting)
            for index in deleted_indices:
                if 0 <= index < len(temp_mappings):
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"🗑️ Deleting index {index}: {temp_mappings[index].get('channel_name', 'Unknown')}")
                    del temp_mappings[index]

            new_mappings = temp_mappings
            deleted_count = len(current_mappings) - len(new_mappings)

            if deleted_count > 0:
                data['mappings'] = new_mappings
                data['last_updated'] = strftime("%Y-%m-%d %H:%M:%S")

                success = self.manual_db.save_database(data)

                if success:
                    # Update local cache
                    self.mappings = new_mappings
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"✅ Deleted {deleted_count} mappings, new total: {len(new_mappings)}")
                    return True

            return False

        except Exception as e:
            logger.error(f"❌ Error performing bulk delete: {str(e)}")
            return False

    def delete_mapping(self):
        """Delete selected mapping with proper GUI update"""
        mapping = self.get_current_mapping()
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"🔍 delete_mapping called, mapping: {mapping is not None}")

        if not mapping:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug("❌ No mapping selected or invalid selection")
            if self.showing_duplicates:
                self.session.open(
                    MessageBox,
                    _("Please select a mapping (not a group header)"),
                    MessageBox.TYPE_INFO,
                    timeout=6
                )
            else:
                self.session.open(
                    MessageBox,
                    _("Please select a mapping to delete"),
                    MessageBox.TYPE_INFO,
                    timeout=6
                )
            return

        channel_name = mapping.get('channel_name', 'Unknown')
        match_type = mapping.get('match_type', 'unknown')

        message = _("Delete manual mapping?\n\nChannel: {}\nType: {}\n\nThis action cannot be undone.").format(
            channel_name, match_type)

        def confirm_callback(result):
            if result is not None and result:
                success = self.perform_delete_mapping(mapping)
                if success:
                    self.changes_made = True
                    self.load_database()

                    if self.showing_duplicates:
                        self.show_duplicates()
                    else:
                        self.show_all_mappings()

                    self["status"].setText(_("Mapping deleted successfully"))
                else:
                    self["status"].setText(_("Error deleting mapping"))

        self.session.openWithCallback(confirm_callback, MessageBox, message, MessageBox.TYPE_YESNO)

    def perform_delete_mapping(self, mapping_to_delete):
        """Actually delete the specified mapping"""
        try:
            data = self.manual_db.load_database()
            current_mappings = data.get('mappings', [])

            new_mappings = []
            deleted = False

            for mapping in current_mappings:
                if (mapping.get('channel_name') == mapping_to_delete.get('channel_name') and
                        mapping.get('clean_name') == mapping_to_delete.get('clean_name') and
                        mapping.get('assigned_sref') == mapping_to_delete.get('assigned_sref')):
                    deleted = True
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"✅ Deleted mapping: {mapping_to_delete.get('channel_name')}")
                else:
                    new_mappings.append(mapping)

            if deleted:
                data['mappings'] = new_mappings
                data['last_updated'] = strftime("%Y-%m-%d %H:%M:%S")

                success = self.manual_db.save_database(data)

                if success:
                    self.mappings = new_mappings
                    return True
                else:
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.error("❌ Failed to save database after deletion")
                    return False
            else:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.warning("⚠️ Mapping not found for deletion")
                return False

        except Exception as e:
            logger.error(f"❌ Error performing delete: {str(e)}")
            return False

    def get_current_mapping(self):
        """Get the current mapping considering duplicates view"""
        selected_index = self["mapping_list"].getSelectedIndex()

        if self.showing_duplicates:
            if selected_index < 0 or selected_index >= len(self.duplicates_data):
                return None
            entry = self.duplicates_data[selected_index]
            if entry[0] == "mapping" and len(entry) >= 3:
                return entry[2]
            return None
        else:
            # Normal vision
            if 0 <= selected_index < len(self.mappings):
                return self.mappings[selected_index]
            return None

    def edit_mapping(self):
        """Edit selected mapping - now called from selection"""
        mapping = self.get_current_mapping()
        if not mapping:
            return

        # SAVE CURRENT VIEW STATE
        current_view = self.previous_view

        # Create channel data for the editor
        channel_data = [{
            'name': mapping.get('channel_name', 'Unknown'),
            'original_name': mapping.get('channel_name', 'Unknown'),
            'sref': mapping.get('assigned_sref', ''),
            'match_type': mapping.get('match_type', 'manual'),
            'tvg_id': mapping.get('tvg_id', ''),
            'url': mapping.get('url', ''),
            'group': 'Manual Database',
            'original_sref': mapping.get('assigned_sref', '')
        }]

        def mapping_editor_closed(result=None):
            """Callback when editor closes"""
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("ManualMatchEditor closed, reloading database...")
            self.load_database()
            self.changes_made = True
            if current_view == "duplicates":
                self.show_duplicates()
            else:
                self.show_all_mappings()

        if config.plugins.m3uconverter.enable_debug.value:
            logger.info(f"Opening ManualMatchEditor for: {mapping.get('channel_name')}")

        self.session.openWithCallback(
            mapping_editor_closed,
            ManualMatchEditor,
            channel_data,
            self.epg_mapper,
            "manual_database_edit"
        )

    def has_unsaved_changes(self):
        """Check if there are unsaved changes"""
        return self.changes_made

    def request_close(self):
        """Handle closing the editor - IMPROVED"""
        try:
            if self.selection_mode:
                # If in selection mode, exit selection first
                self.exit_selection_mode()
                return

            if self.showing_duplicates:
                # If in duplicates view, return to normal view
                self.show_all_mappings()
                return

            # Otherwise, close the editor
            self.close()

        except Exception as e:
            logger.error(f"Error in request_close: {str(e)}")
            self.close()

    def ask_save_before_close(self):
        """Ask user if they want to save before closing"""
        if not self.has_unsaved_changes():
            self.close()
            return

        message = _("You have unsaved changes in the database.\n\nDo you want to save before closing?")

        def callback(result):
            if result:
                # User said YES to save
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("💾 User chose to save changes before closing")
                if self.save_all_changes():
                    self.close()
                else:
                    self.close()
            else:
                # User said NO - close without saving
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("❌ User chose to close without saving")
                self.close()

        self.session.openWithCallback(
            callback,
            MessageBox,
            message,
            MessageBox.TYPE_YESNO,
            timeout=15,
            default=True
        )

    def do_final_close(self):
        """Final close procedure - ALWAYS close properly"""
        try:
            if config.plugins.m3uconverter.enable_debug.value:
                logger.debug("ManualDatabaseEditor: Final closure")
            # Reset changes flag
            self.changes_made = False
            # Close the screen - use correct super class
            self.close()

        except Exception as e:
            logger.error(f"Error in do_final_close: {str(e)}")
            self.close()

    def keyCancel(self):
        """Handle the CANCEL key — manages hierarchical navigation."""
        if self.showing_duplicates:
            # If we are in the duplicates view, return to the normal view
            self.show_all_mappings()
        else:
            # If we are in the main view, close the editor
            self.request_close()


class ManualDatabaseManager:

    def __init__(self):
        logger.info("✅ Manual database manager initialized")
        self.db_path = DB_PATCH
        logger.info(f"✅ Manual database path: {self.db_path}")
        self._save_lock = threading.Lock()
        self._cached_db = None
        self._cache_timestamp = 0
        self._ensure_db_directory()
        self._ensure_db_file()
        self.cleanup_inconsistent_data()
        self._ensure_db_integrity()
        self.fix_existing_mappings()
        """
        try:
            self.load_database()
        except:
            logger.error("Database severely corrupted, attempting emergency repair")
            self.emergency_repair_database()
        logger.info(f"✅ Manual database path: {self.db_path}")
        """

    def _ensure_db_directory(self):
        """Create the database directory if it does not exist"""
        db_dir = dirname(self.db_path)
        if not exists(db_dir):
            makedirs(db_dir, exist_ok=True)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"📁 Created database directory: {db_dir}")

    def _get_default_structure(self):
        """Return default database structure"""
        return {
            "version": CURRENT_VERSION,
            "last_updated": strftime("%Y-%m-%d %H:%M:%S"),
            "mappings": []
        }

    def _ensure_db_file(self):
        """Create DB file if it does not exist"""
        if not exists(self.db_path):
            try:
                initial_data = self._get_default_structure()
                with open(self.db_path, 'w', encoding='utf-8') as f:
                    json.dump(initial_data, f, indent=2, ensure_ascii=False)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"✅ Created manual database: {self.db_path}")

                chmod(self.db_path, 0o644)
            except Exception as e:
                logger.error(f"❌ Cannot create manual database: {str(e)}")
                # Fallback a /tmp
                self.db_path = "/tmp/manual_mappings.json"
                if not exists(self.db_path):
                    with open(self.db_path, 'w', encoding='utf-8') as f:
                        json.dump(initial_data, f, indent=2, ensure_ascii=False)

    def load_database(self):
        """Load the database from memory with enhanced error recovery AND CACHE"""

        # CACHE CHECK - if already loaded, return cached version
        if hasattr(self, '_cached_db') and self._cached_db is not None:
            return self._cached_db

        try:
            # If the file doesn’t exist, create a default structure
            if not exists(self.db_path):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("Database file not found, creating default structure")
                data = self._get_default_structure()
                self._cached_db = data  # ✅ SAVE TO CACHE
                return data

            # Read file content
            with open(self.db_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()

            # Handle empty file
            if not content:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.warning("Database file is empty, creating default structure")
                data = self._get_default_structure()
                self._cached_db = data  # ✅ SAVE TO CACHE
                return data

            # Try parsing JSON
            try:
                data = json.loads(content)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.debug("✅ Database loaded successfully and cached")
                self._cached_db = data  # ✅ SAVE TO CACHE
                return data

            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON parse error: {str(e)}")
                logger.error(f"Error at position {e.pos}: {e.msg}")

                # Try to fix the JSON content
                fixed_content = self._fix_json_errors(content)

                try:
                    data = json.loads(fixed_content)
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info("✅ JSON errors fixed automatically")

                    # Save the fixed version to prevent future issues
                    self._save_fixed_database(data)
                    self._cached_db = data  # ✅ SAVE TO CACHE
                    return data

                except json.JSONDecodeError as e2:
                    logger.error(f"❌ Could not fix JSON errors: {e2}")
                    logger.error("Using default database structure")

                    # Backup the corrupted file
                    if exists(self.db_path):
                        corrupt_backup = f"{self.db_path}.corrupted_{strftime('%Y%m%d_%H%M%S')}"
                        shutil.copy2(self.db_path, corrupt_backup)
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info(f"Backed up corrupted file to: {corrupt_backup}")

                    data = self._get_default_structure()
                    self._cached_db = data  # ✅ SAVE TO CACHE
                    return data

            # Normalize data after loading
            if 'mappings' in data:
                normalized_mappings = []
                for mapping in data['mappings']:
                    if isinstance(mapping, dict):
                        # Ensure all mappings have a consistent structure
                        normalized_mapping = {
                            'channel_name': mapping.get('channel_name', ''),
                            'original_name': mapping.get('original_name', mapping.get('channel_name', '')),
                            'clean_name': mapping.get('clean_name', ''),
                            'tvg_id': mapping.get('tvg_id', ''),
                            'assigned_sref': mapping.get('assigned_sref', ''),
                            'match_type': mapping.get('match_type', 'manual_db'),
                            'similarity': mapping.get('similarity', 1.0),
                            'bouquet_source': mapping.get('bouquet_source', 'unknown'),
                            'created': mapping.get('created', ''),
                            'last_used': mapping.get('last_used', '')
                        }

                        # Remove original_sref if it exists
                        if 'original_sref' in mapping:
                            del mapping['original_sref']
                        normalized_mappings.append(normalized_mapping)

                data['mappings'] = normalized_mappings

            self._cached_db = data  # SAVE TO CACHE
            return data

        except Exception as e:
            logger.error(f"❌ Critical error loading manual database: {str(e)}")
            data = self._get_default_structure()
            self._cached_db = data  # SAVE TO CACHE
            return data

    def find_mapping(self, channel_name, tvg_id=None, clean_name=None):
        """ULTRA OPTIMIZED version with pre-built indexes"""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"🔍 MANUAL DB FIND_MAPPING: '{channel_name}' -> '{clean_name}'")
        try:
            if not hasattr(self, '_manual_cache'):
                self._manual_cache = {}

            cache_key = f"{clean_name}_{tvg_id}"
            if cache_key in self._manual_cache:
                return self._manual_cache[cache_key]

            # BUILD INDEXES ON FIRST CALL
            if not hasattr(self, '_manual_indexes_built'):
                self._build_manual_indexes()

            # O(1) LOOKUPS
            if clean_name and clean_name in self._clean_name_index:
                mapping = self._clean_name_index[clean_name]
                self._manual_cache[cache_key] = mapping
                return mapping

            if channel_name and channel_name.lower() in self._channel_name_index:
                mapping = self._channel_name_index[channel_name.lower()]
                self._manual_cache[cache_key] = mapping
                return mapping

            if tvg_id and tvg_id in self._tvg_id_index:
                mapping = self._tvg_id_index[tvg_id]
                self._manual_cache[cache_key] = mapping
                return mapping

            self._manual_cache[cache_key] = None
            return None

        except Exception as e:
            logger.error(f'find_mapping error: {str(e)}')
            return None

    def save_manual_mapping(self, mapping_data, immediate=True):
        """Save mapping to database with anti-loop check"""
        if not hasattr(self, '_save_lock'):
            self._save_lock = threading.Lock()

        with self._save_lock:
            try:
                # ANTI-LOOP CHECK: verify if this is a recursive auto-save
                if mapping_data.get('bouquet_source') == 'auto_save':
                    # Check if an identical mapping already exists
                    existing = self.find_mapping(
                        mapping_data.get('channel_name'),
                        mapping_data.get('tvg_id'),
                        mapping_data.get('clean_name')
                    )
                    if existing and existing.get('assigned_sref') == mapping_data.get('assigned_sref'):
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.debug(f"🔄 Auto-save skip: identical mapping already exists for {mapping_data.get('channel_name')}")
                        return True  # Do not save duplicates

                # Check that assigned_sref is not an encoded URL
                assigned_sref = mapping_data.get('assigned_sref', '')
                if assigned_sref and assigned_sref.startswith('http'):
                    # This is a URL, not a valid service reference – convert it properly
                    if hasattr(self, 'epg_mapper') and self.epg_mapper:
                        mapping_data['assigned_sref'] = self.epg_mapper._generate_service_reference(assigned_sref)
                    else:
                        # Fallback: generate an IPTV-style service reference
                        url_hash = hashlib.md5(assigned_sref.encode('utf-8')).hexdigest()[:8]
                        service_id = int(url_hash, 16) % 65536
                        encoded_url = assigned_sref.replace(':', '%3a').replace(' ', '%20')
                        mapping_data['assigned_sref'] = f"4097:0:1:{service_id}:0:0:0:0:0:0:{encoded_url}"

                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"💾 SAVE: {mapping_data.get('channel_name')} -> {mapping_data.get('assigned_sref')}")

                if not mapping_data.get('assigned_sref') or not mapping_data.get('channel_name'):
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.error("❌ Invalid mapping data")
                    return False

                # Load and update data
                data = self.load_database()
                mappings = data.get("mappings", [])

                clean_name = mapping_data.get('clean_name', '').lower()
                updated = False

                for i, existing in enumerate(mappings):
                    if existing.get('clean_name', '').lower() == clean_name:
                        mappings[i] = mapping_data
                        updated = True
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info(f"💾 Updated: {clean_name}")
                        break

                if not updated:
                    mappings.append(mapping_data)
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.info(f"💾 Added: {clean_name}")

                data['mappings'] = mappings
                data['last_updated'] = strftime("%Y-%m-%d %H:%M:%S")

                # IMMEDIATE save with verification
                success = self.save_database(data)

                if success:
                    logger.info(f"✅ Saved successfully: {mapping_data.get('channel_name')}")
                else:
                    logger.error(f"❌ Save failed: {mapping_data.get('channel_name')}")

                return success

            except Exception as e:
                logger.error(f"❌ Manual mapping save error: {str(e)}")
                import traceback
                logger.error(f"❌ Traceback: {traceback.format_exc()}")
                return False

    def _enforce_db_size_limit(self):
        """Enforce manual database size limit - SENZA ricorsione"""
        if not config.plugins.m3uconverter.use_manual_database.value:
            return

        max_size = config.plugins.m3uconverter.manual_db_max_size.value

        try:
            # Load data directly from the file to avoid recursion
            with open(self.db_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            mappings = data.get('mappings', [])

            if len(mappings) > max_size:
                # Sort by usage count and keep most used
                mappings.sort(key=lambda x: x.get('usage_count', 0))
                data['mappings'] = mappings[-max_size:]
                data['last_updated'] = strftime("%Y-%m-%d %H:%M:%S")

                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"📏 Enforced DB size limit: {len(mappings)} -> {max_size}")

                # Save directly without calling save_database
                temp_path = f"{self.db_path}.tmp"
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                replace(temp_path, self.db_path)

        except Exception as e:
            logger.error(f"❌ Error enforcing DB size limit: {str(e)}")

    def save_database(self, data):
        """Save database to file with enhanced debugging"""
        try:
            # DEBUG: Log what we're trying to save
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("💾 SAVE DATABASE DEBUG: Starting save process")
                logger.info(f"   Data type: {type(data)}")
                logger.info(f"   Mappings count: {len(data.get('mappings', []))}")

            # Validate data structure before saving
            if not isinstance(data, dict) or 'mappings' not in data:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("❌ Invalid data structure for saving")
                return False

            # Validate each mapping
            valid_mappings = []
            corrupted_count = 0

            for i, mapping in enumerate(data.get('mappings', [])):
                if (isinstance(mapping, dict) and
                        mapping.get('channel_name') and
                        mapping.get('assigned_sref')):

                    # Remove original_sref if it exists
                    if 'original_sref' in mapping:
                        del mapping['original_sref']

                    valid_mappings.append(mapping)
                else:
                    corrupted_count += 1
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.warning(f"⚠️ Corrupted mapping at index {i}: {mapping}")

            if corrupted_count > 0:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.warning(f"⚠️ Found {corrupted_count} corrupted mappings")

            data['mappings'] = valid_mappings
            data['last_updated'] = strftime("%Y-%m-%d %H:%M:%S")

            # DEBUG: Log before file operations
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"💾 About to write {len(valid_mappings)} mappings to file")

            # Save with atomic write and verification
            temp_path = f"{self.db_path}.tmp"

            # DEBUG: Log the JSON we're about to write
            try:
                json_str = json.dumps(data, indent=2, ensure_ascii=False)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"💾 JSON string length: {len(json_str)}")
                    logger.info(f"💾 First 500 chars: {json_str[:500]}")
                    logger.info(f"💾 Last 500 chars: {json_str[-500:]}")
            except Exception as e:
                logger.error(f"❌ JSON serialization failed: {e}")

            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # Verify the saved file can be read back
            try:
                with open(temp_path, 'r', encoding='utf-8') as f:
                    saved_content = f.read()
                    if config.plugins.m3uconverter.enable_debug.value:
                        verified_data = json.loads(saved_content)  # Test reading
                        logger.info(f"✅ Saved file verification PASSED: {len(verified_data.get('mappings', []))} mappings loaded successfully")
            except json.JSONDecodeError as e:
                logger.error(f"❌ Saved file verification FAILED: {str(e)}")
                logger.error(f"❌ Corrupted content: {saved_content[e.pos - 50:e.pos + 50]}")
                return False

            # Replace original file
            replace(temp_path, self.db_path)

            # APPLY SIZE LIMIT AFTER SAVING
            # But only if we've actually saved data
            """
            if len(valid_mappings) > 0:
                self._enforce_db_size_limit()
            """
            logger.info("✅ Database saved successfully")

            return True

        except Exception as e:
            logger.error(f"❌ Enhanced save error: {str(e)}")
            import traceback
            logger.error(f"❌ Traceback: {traceback.format_exc()}")
            return False

    def _create_single_backup(self):
        """Create a single backup with timestamp"""
        try:
            if not exists(self.db_path):
                return False

            timestamp = strftime("%Y%m%d_%H%M%S")
            backup_filename = f"{basename(self.db_path)}.backup_{timestamp}"
            backup_path = join(dirname(self.db_path), backup_filename)

            # Copy current database to backup
            shutil.copy2(self.db_path, backup_path)

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"💾 Created backup: {backup_filename}")

            return True

        except Exception as e:
            logger.error(f"❌ Backup creation error: {str(e)}")
            return False

    def _build_manual_indexes(self):
        """Build fast lookup indexes for manual database"""
        data = self.load_database()
        mappings = data.get("mappings", [])

        self._clean_name_index = {}
        self._channel_name_index = {}
        self._tvg_id_index = {}

        for mapping in mappings:
            clean = mapping.get('clean_name')
            channel = mapping.get('channel_name')
            tvg = mapping.get('tvg_id')

            if clean:
                self._clean_name_index[clean] = mapping
            if channel:
                self._channel_name_index[channel.lower()] = mapping
            if tvg:
                self._tvg_id_index[tvg] = mapping

        self._manual_indexes_built = True
        if config.plugins.m3uconverter.enable_debug.value:
            logger.debug(f"✅ Built manual DB indexes: {len(mappings)} mappings")

    def cleanup_inconsistent_data(self):
        """Remove inconsistent entries from the manual database"""
        try:
            data = self.load_database()
            mappings = data.get("mappings", [])

            cleaned_mappings = []
            removed_count = 0

            for mapping in mappings:
                # Check that the mapping has the correct structure
                if not isinstance(mapping, dict):
                    removed_count += 1
                    continue

                # Check for mandatory fields
                if not mapping.get('channel_name') or not mapping.get('assigned_sref'):
                    removed_count += 1
                    continue

                # Remove the 'original_sref' field if present (optional)
                if 'original_sref' in mapping:
                    del mapping['original_sref']

                cleaned_mappings.append(mapping)

            if removed_count > 0:
                data['mappings'] = cleaned_mappings
                data['last_updated'] = strftime("%Y-%m-%d %H:%M:%S")
                self.save_database(data)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"🧹 Cleaned {removed_count} inconsistent mappings from database")

            return removed_count

        except Exception as e:
            logger.error(f"Error cleaning inconsistent data: {str(e)}")
            return 0

    def emergency_repair_database(self):
        """Emergency repair for severely corrupted database"""
        try:
            if not exists(self.db_path):
                return True

            # Read the file
            with open(self.db_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Create emergency backup
            emergency_backup = f"{self.db_path}.emergency_backup_{strftime('%Y%m%d_%H%M%S')}"
            with open(emergency_backup, 'w', encoding='utf-8') as f:
                f.write(content)

            # Try to extract valid mappings using regex
            mappings_pattern = r'\{[^{}]*"channel_name"[^{}]*\}'
            potential_mappings = findall(mappings_pattern, content)

            valid_mappings = []
            for mapping_str in potential_mappings:
                try:
                    # Clean up the mapping string
                    mapping_str = sub(r',\s*}', '}', mapping_str)  # Remove trailing commas
                    mapping_str = sub(r'"\s*"', '": "', mapping_str)  # Fix missing colons

                    mapping = json.loads(mapping_str)
                    if isinstance(mapping, dict) and mapping.get('channel_name'):
                        valid_mappings.append(mapping)
                except:
                    continue

            # Create new database structure
            new_data = self._get_default_structure()
            new_data["mappings"] = valid_mappings

            # Save the repaired database
            with open(self.db_path, 'w', encoding='utf-8') as f:
                json.dump(new_data, f, indent=2, ensure_ascii=False)

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"✅ Emergency repair completed. Recovered {len(valid_mappings)} mappings.")
            return True

        except Exception as e:
            logger.error(f"❌ Emergency repair failed: {str(e)}")
            # If all else fails, create a clean database
            clean_data = self._get_default_structure()
            with open(self.db_path, 'w', encoding='utf-8') as f:
                json.dump(clean_data, f, indent=2, ensure_ascii=False)
            return True

    def _save_fixed_database(self, data):
        """Save the fixed database with backup"""
        try:
            # Create backup before saving
            if exists(self.db_path):
                backup_path = f"{self.db_path}.backup_{strftime('%Y%m%d_%H%M%S')}"
                shutil.copy2(self.db_path, backup_path)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"Created backup: {backup_path}")

            # Save fixed database
            with open(self.db_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info("Fixed database saved successfully")

            return True
        except Exception as e:
            logger.error(f"Error saving fixed database: {str(e)}")
            return False

    def _ensure_db_integrity(self):
        """Check the integrity of the database periodically"""
        try:
            data = self.load_database()

            # Verify that the structure is valid
            if not isinstance(data, dict):
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("❌ Database structure corrupted - not a dict")
                return False

            if 'mappings' not in data:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.error("❌ Database missing 'mappings' key")
                return False

            # Check each mapping
            valid_count = 0
            for i, mapping in enumerate(data.get('mappings', [])):
                if not isinstance(mapping, dict):
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.warning(f"⚠️ Mapping {i} is not a dict: {mapping}")
                    continue

                if not mapping.get('channel_name') or not mapping.get('assigned_sref'):
                    if config.plugins.m3uconverter.enable_debug.value:
                        logger.warning(f"⚠️ Mapping {i} missing required fields: {mapping}")
                    continue

                valid_count += 1

            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"✅ Database integrity check: {valid_count} valid mappings")
            return True

        except Exception as e:
            logger.error(f"❌ Database integrity check failed: {e}")
            return False

    def _fix_json_errors(self, content):
        """Enhanced JSON error fixing with better delimiter handling"""
        try:
            # Debug log
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"🔧 JSON fix started, length: {len(content)}")

            if len(content) > 50000:
                logger.warning("📏 JSON too large, may be corrupted")

            # Find where the problem starts
            try:
                json.loads(content)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info("✅ JSON was already valid")
                return content
            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON error at char {e.pos}: {e.msg}")
                # Log area around the error
                start = max(0, e.pos - 100)
                end = min(len(content), e.pos + 100)
                error_context = content[start:end]
                logger.error(f"🔍 Error context: ...{error_context}...")

            if not content or not isinstance(content, str):
                return json.dumps(self._get_default_structure())

            # Strategy 1: Fix missing colons between keys and values
            # More precise pattern to find key-value pairs missing colons
            fixed = sub(r'"\s*"', '": "', content)

            # Strategy 2: Fix specific missing colon patterns
            # Look for patterns like: "key"  "value" or "key" "value"
            fixed = sub(r'"\s*"([^"])', '": "\\1', fixed)

            # Strategy 3: Fix missing colons after keys
            fixed = sub(r'("[^"]*")\s*(")', '\\1: \\2', fixed)

            # Strategy 4: Remove trailing commas before ] or }
            fixed = sub(r',\s*([\]}])', r'\1', fixed)

            # Strategy 5: Fix missing commas between objects
            fixed = sub(r'}[\s\n]*{', '},{', fixed)

            # Strategy 6: Balance braces
            open_braces = fixed.count('{')
            close_braces = fixed.count('}')

            if open_braces > close_braces:
                fixed += '}' * (open_braces - close_braces)
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.info(f"Added {open_braces - close_braces} missing closing braces")
            elif close_braces > open_braces:
                # Remove extra closing braces from end
                while close_braces > open_braces and fixed.endswith('}'):
                    fixed = fixed[:-1]
                    close_braces -= 1

            # Strategy 7: Fix unescaped quotes in strings
            # Only fix quotes that are inside strings but not properly escaped
            lines = fixed.split('\n')
            fixed_lines = []
            for line in lines:
                # Count quotes in the line
                quote_count = line.count('"')
                if quote_count % 2 != 0 and '"channel_name"' in line:
                    # This might be a line with unescaped quotes
                    line = sub(r'(?<!\\)"(?!\s*[:,\]})])', r'\\"', line)
                fixed_lines.append(line)
            fixed = '\n'.join(fixed_lines)

            # Strategy 8: Ensure proper structure
            fixed = self._ensure_json_structure(fixed)

            return fixed

        except Exception as e:
            logger.error(f"Enhanced JSON fix error: {str(e)}")
            # Return a clean default structure if all fixes fail
            return json.dumps(self._get_default_structure())

    def _ensure_json_structure(self, content):
        """Ensure the JSON has proper structure and fix common issues"""
        try:
            # Find the main JSON object
            first_brace = content.find('{')
            last_brace = content.rfind('}')

            if first_brace == -1 or last_brace == -1:
                if config.plugins.m3uconverter.enable_debug.value:
                    logger.warning("No valid JSON structure found, creating default")
                return json.dumps(self._get_default_structure())

            # Extract the main content between first { and last }
            json_content = content[first_brace:last_brace + 1]

            # Fix common pattern: "key" "value" should be "key": "value"
            json_content = sub(r'("[^"]+")\s+"([^"]+)"', r'\1: "\2"', json_content)

            # Fix missing commas between objects in mappings array
            json_content = sub(r'"\s*}\s*"', '"}, "', json_content)

            return json_content

        except Exception as e:
            logger.error(f"JSON structure fix error: {str(e)}")
            return content

    def flush(self):
        """Flush - now it's a no-op because we always save immediately"""
        if config.plugins.m3uconverter.enable_debug.value:
            logger.info("💾 Flush called (no-op with immediate saving)")
        return True

    def _cleanup_old_backups(self):
        """Clean up old backup files (keep only latest 3)"""
        try:
            backup_dir = dirname(self.db_path)  # PLUGIN_PATH + "/database"

            # Search ONLY for backups of the main file, not exports
            backup_pattern = join(backup_dir, "manual_mappings.json.backup_*")
            backups = sorted(glob.glob(backup_pattern))

            # DEBUG: Log found backups
            if config.plugins.m3uconverter.enable_debug.value:
                logger.info(f"🔍 Found {len(backups)} backup files:")
                for backup in backups:
                    logger.info(f"   - {basename(backup)}")

            # Keep only the last 3 backups
            if len(backups) > 3:
                backups_to_remove = backups[:-3]  # All except the last 3
                for backup in backups_to_remove:
                    try:
                        remove(backup)
                        if config.plugins.m3uconverter.enable_debug.value:
                            logger.info(f"🧹 Removed old backup: {basename(backup)}")
                    except Exception as e:
                        logger.error(f"❌ Error removing backup {backup}: {str(e)}")

        except Exception as e:
            logger.error(f"Backup cleanup error: {str(e)}")

    def fix_existing_mappings(self):
        """Fix existing mappings that contain URLs instead of service references"""
        try:
            data = self.load_database()
            mappings = data.get('mappings', [])
            fixed_count = 0
            for mapping in mappings:
                assigned_sref = mapping.get('assigned_sref', '')
                if assigned_sref and assigned_sref.startswith('http'):
                    # Convert URL to service reference
                    url = assigned_sref
                    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:8]
                    service_id = int(url_hash, 16) % 65536
                    encoded_url = url.replace(':', '%3a').replace(' ', '%20')
                    mapping['assigned_sref'] = f"4097:0:1:{service_id}:0:0:0:0:0:0:{encoded_url}"
                    fixed_count += 1
                    logger.info(f"🔧 Fixed mapping: {mapping.get('channel_name')}")

            if fixed_count > 0:
                data['mappings'] = mappings
                self.save_database(data)
                logger.info(f"✅ Fixed {fixed_count} mappings with URL instead of service reference")

            return fixed_count

        except Exception as e:
            logger.error(f"❌ Error fixing existing mappings: {str(e)}")
            return 0


def main(session, **kwargs):
    """Main entry point with storage verification"""
    if config.plugins.m3uconverter.enable_debug.value:
        logger.info(f"🚀 Plugin starting, storage: {ARCHIMEDE_CONVERTER_PATH}")
        core_converter._log_current_configuration
        core_converter.cleanup_old_backups(config.plugins.m3uconverter.max_backups.value)
    session.open(ConversionSelector)


def Plugins(**kwargs):
    from Plugins.Plugin import PluginDescriptor
    return [PluginDescriptor(
        name=_("Universal Converter"),
        description=_("Convert between M3U Enigma2 Bouquets Json"),
        where=PluginDescriptor.WHERE_PLUGINMENU,
        icon="plugin.png",
        fnc=main)
    ]


# Global converter instance
core_converter = CoreConverter()
