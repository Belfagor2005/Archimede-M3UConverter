# 📺 Archimede M3U Converter

A simple and powerful playlist converter for **Enigma2 (Python 3 only)**.
Easily turn `.m3u`, `.json`, and `.xspf` files into clean IPTV lists with group, logo, and stream info — and export Enigma2 bouquets back to `.m3u`!

```
## 🚀 Features

- ✅ Choose playlist files from USB, HDD, or network shares (`.m3u`, `.tv`, `.json`, `.xspf`)
- 🔍 Parses EXTINF data: title, logo, group-title, tvg-name
- 🧹 Cleans and exports a minimal Enigma2-compatible bouquet
- 🔁 Convert streams to **HLS** (optional)
- 📤 Export Enigma2 bouquets (`.tv`) back to `.m3u` format
- 💾 Automatic backup of original files
- 🧠 Auto-reload playlist at boot
- 📚 Add new bouquets at top or bottom of main list
- 📂 File browser with smart filter: shows only `.tv` files containing HTTP streams
- 🔄 Convert JSON IPTV playlist to Enigma2 bouquets
- 🔄 Convert XSPF playlists to M3U format
- 🔄 **Convert M3U playlists to JSON** (structured channels metadata)
- 🗓️ **When converting to `.tv`, auto-attach EPG info if available** (based on `tvg-id`/`tvg-name`)



## 🎮 How to Use

| Button       | Action                                      |
|--------------|---------------------------------------------|
| 🔴 RED       | Open file (M3U, TV, JSON, XSPF)             |
| 🟢 GREEN     | Start conversion                            |
| 🟡 YELLOW    | Toggle stream filter (HTTP-only .tv files)  |
| 🔵 BLUE      | Tools menu (export, preview, etc.)          |
| 📂 MENU      | Plugin settings (HLS, autoload, position)   |
| ▶️  OK        | Play selected channel                       |
| ⏹️ STOP      | Stop playback                               |
| ❌ CANCEL    | Close the plugin                            |



## 🗂️ Output

- Converted `.m3u` ➜ saved to:
  - `/etc/enigma2/userbouquet.*.tv` (with EPG mapping when available)
- Converted `.tv` ➜ exported to:
  - `/tmp/exported_*.m3u`
- Converted `.json` ➜ saved as:
  - `/etc/enigma2/userbouquet.*.tv`
- Converted `.xspf` ➜ exported as:
  - `/tmp/converted_*.m3u`
- **Converted `.m3u` ➜ exported as structured `.json`:**
  - `/tmp/converted_*.json`
- UTF-8 M3U support ✅


## 🔄 Reverse Conversion

You can now export Enigma2 bouquets (`userbouquet.*.tv`) back into `.m3u` format:

- Output saved in: `/tmp/exported_*.m3u`
- Each channel is saved with its original name and URL
- Only valid IPTV entries are included (e.g., `#SERVICE 4097`, `5001`, etc.)
- Non-stream services (DVB, radio, etc.) are ignored

New supported formats:

- `.json` ➜ Enigma2 bouquet conversion
- `.xspf` ➜ converted to `.m3u` playlist
- `.m3u` ➜ **converted to `.json`**

Perfect for creating backup playlists or re-editing outside Enigma2.



## 💡 Notes

- Works only on **Python 3** images
- Designed for modern Enigma2 boxes
- No internet required for conversion
- Clean, local processing — no logs or tracking
- EPG attachment uses available tags (`tvg-id`, `tvg-name`) to map channels when present



## 📄 Credits & License

Created by **Belfagor2005** (Archimede).  
All rights reserved. Redistribution only with explicit credit.

This plugin is made with ❤️ for the Enigma2 community.  
You are free to use and modify it for personal use.

🚫 **Do not redistribute modified versions without proper attribution.**
```


