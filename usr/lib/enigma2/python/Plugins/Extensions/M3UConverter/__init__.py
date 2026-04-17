# -*- coding: utf-8 -*-
from __future__ import absolute_import
from Tools.Directories import resolveFilename, SCOPE_PLUGINS
from Components.Language import language
import gettext

__author__ = "Lululla"
__email__ = "ekekaz@gmail.com"
__copyright__ = "Copyright (c) 2024 Lululla"
__license__ = "GPL-v2"
__version__ = "3.3"
LAST_MODIFIED_DATE = "20260417"

PluginLanguageDomain = "M3UConverter"
PluginLanguagePath = "Extensions/M3UConverter/locale"

PluginLanguageDomain = "M3UConverter"
PluginLanguagePath = "Extensions/M3UConverter/locale"


def localeInit():
    gettext.bindtextdomain(
        PluginLanguageDomain,
        resolveFilename(
            SCOPE_PLUGINS,
            PluginLanguagePath))


def _(txt):
    if gettext.dgettext(PluginLanguageDomain, txt):
        return gettext.dgettext(PluginLanguageDomain, txt)
    else:
        print(("[%s] fallback to default translation for %s" %
              (PluginLanguageDomain, txt)))
        return gettext.gettext(txt)


localeInit()
language.addCallback(localeInit)
