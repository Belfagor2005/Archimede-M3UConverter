# -*- coding: utf-8 -*-
from __future__ import absolute_import

__author__ = "Lululla"
__email__ = "ekekaz@gmail.com"
__copyright__ = "Copyright (c) 2024 Lululla"
__license__ = "GPL-v2"
__version__ = "3.2"

import os
import gettext

from Components.Language import language
from Tools.Directories import resolveFilename, SCOPE_PLUGINS

PluginLanguageDomain = "M3UConverter"
PluginLanguagePath = "Extensions/M3UConverter/locale"

PluginLanguageDomain = "M3UConverter"
PluginLanguagePath = "Extensions/M3UConverter/locale"

# Determine if the system is running DreamOS
isDreamOS = os.path.exists("/var/lib/dpkg/status")


def localeInit():
    if isDreamOS:  # check if opendreambox image
        # getLanguage returns e.g. "fi_FI" for "language_country"
        lang = language.getLanguage()[:2]
        # Enigma doesn't set this (or LC_ALL, LC_MESSAGES, LANG). gettext needs
        # it!
        os.environ["LANGUAGE"] = lang
    gettext.bindtextdomain(
        PluginLanguageDomain,
        resolveFilename(
            SCOPE_PLUGINS,
            PluginLanguagePath))


# Define the _ function based on whether it's DreamOS or not
if isDreamOS:  # check if DreamOS image
    def _(txt):
        return gettext.dgettext(PluginLanguageDomain, txt) if txt else ""
else:
    def _(txt):
        if gettext.dgettext(PluginLanguageDomain, txt):
            return gettext.dgettext(PluginLanguageDomain, txt)
        else:
            print(("[%s] fallback to default translation for %s" %
                  (PluginLanguageDomain, txt)))
            return gettext.gettext(txt)

localeInit()
language.addCallback(localeInit)
