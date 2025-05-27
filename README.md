Archimede-M3UConverter 
 

![](https://komarev.com/ghpvc/?username=Belfagor2005) [![Python package](https://github.com/Belfagor2005/Archimede-M3UConverter/actions/workflows/pylint.yml/badge.svg)](https://github.com/Belfagor2005/Archimede-M3UConverter/actions/workflows/pylint.yml)

<img src="https://github.com/Belfagor2005/Archimede-M3UConverter/blob/main/usr/lib/enigma2/python/Plugins/Extensions/M3UConverter/plugin.png">


Made with ❤️ for Archimede.


```markdown
# 📺 Archimede M3U Converter

A simple and powerful M3U playlist converter for **Enigma2 (Python 3 only)**.  
Easily turn `.m3u` files into clean IPTV lists with group, logo, and stream info — and export Enigma2 bouquets back to `.m3u`!

---

## 🚀 Features

- ✅ Choose `.m3u` files from USB, HDD, or network shares
- 🔍 Parses EXTINF data: title, logo, group-title, tvg-name
- 🧹 Cleans and exports a minimal Enigma2-compatible list
- 🔁 Optionally convert streams to **HLS**
- 📤 Also export bouquet `.tv` files back to `.m3u`
- 💾 Automatic backup of original files
- 🧠 Auto-reload playlist at boot
- 📚 Add new bouquets at top or bottom of main list

---

## 🎮 How to Use

| Button       | Action                                      |
|--------------|---------------------------------------------|
| 🔴 RED       | Select M3U/TV file                             |
| 🟢 GREEN     | Convert M3U to bouquet or bouquet to M3U    |
| 🟡 YELLOW    | Settings (HLS, backup, autoload, position)  |
| 🔵 BLUE      | Plugin info and about                       |

---

## 🗂️ Output

- Converted `.m3u` ➜ saved to:
  - `/etc/enigma2/userbouquet.*.tv`
- Converted `.tv` ➜ exported to:
  - `/tmp/exported_*.m3u`
- UTF-8 M3U support ✅

---

## 🔄 Reverse Conversion

You can now export Enigma2 bouquets (`userbouquet.*.tv`) back into `.m3u` format:

- Output saved in: `/tmp/exported_*.m3u`
- Each channel is saved with its original name and URL
- Only valid IPTV entries are included (e.g., `#SERVICE 4097`, `5001`, etc.)
- Non-stream services (DVB, radio, etc.) are ignored

Perfect for creating backup playlists or re-editing outside Enigma2.

---

## 💡 Notes

- Works only on **Python 3** images
- Designed for modern Enigma2 boxes
- No internet required for conversion
- Clean, local processing — no logs or tracking

---

## 📄 Credits & License

Created by **Belfagor2005** (Archimede).  
All rights reserved. Redistribution only with explicit credit.

This plugin is made with ❤️ for the Enigma2 community.  
You are free to use and modify it for personal use.

🚫 **Do not redistribute modified versions without proper attribution.**

```
