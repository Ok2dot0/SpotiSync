import os
import re
import sys
import json
import signal
import shutil
import threading
import subprocess
import configparser
import time
from pathlib import Path
from tqdm import tqdm

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from mutagen.id3 import ID3, TXXX
import concurrent.futures
import lyricsgenius  # new import for lyrics

class SpotifySync:
    def __init__(self):
        self.running = True
        self.paused = False
        self.state_file = Path('.sync_state.json')
        self.current_state = {}
        self.lock = threading.Lock()
        self.main_pbar = None
        self.song_map_path = Path("song_mapping.json")
        self.song_map = {}

    def signal_handler(self, sig, frame):
        self.log('WARN', "Ctrl+C detected. Stopping gracefully...")
        self.running = False
        self.save_state()
        sys.exit(0)

    def save_state(self):
        with self.lock:
            state = self.load_state()
            state.update(self.current_state)
            with open(self.state_file, 'w') as f:
                json.dump(state, f)

    def load_state(self):
        return json.loads(self.state_file.read_text()) if self.state_file.exists() else {}

    def clear_state(self):
        if self.state_file.exists():
            self.state_file.unlink()

    def sanitize_name(self, name):
        return re.sub(r'[\\/*?:"<>|]', '_', name).strip()

    def read_settings(self):
        config = configparser.ConfigParser()
        config.read('settings.ini')
        return {
            'client_id': config['DEFAULT']['CLIENT_ID'],
            'client_secret': config['DEFAULT']['CLIENT_SECRET'],
            'redirect_uri': config['DEFAULT']['REDIRECT_URI'],
            'download_liked': config['SETTINGS'].getboolean('DOWNLOAD_LIKED', False),
            'root_dir': config['SETTINGS'].get('ROOT_DIR', 'Spotify Playlists'),
            'cache_dir': config['SETTINGS'].get('CACHE_DIR', 'spotify_cache'),
            'parallel_downloads': config['SETTINGS'].getint('PARALLEL_DOWNLOADS', 2),
            'genius_token': config['DEFAULT'].get('GENIUS_TOKEN', '').strip()  # new setting
        }

    def get_spotify_client(self, settings):
        auth_manager = SpotifyOAuth(
            client_id=settings['client_id'],
            client_secret=settings['client_secret'],
            redirect_uri=settings['redirect_uri'],
            scope='playlist-read-private playlist-read-collaborative user-library-read'
        )
        return spotipy.Spotify(auth_manager=auth_manager)

    def get_all_playlists(self, sp):
        playlists = []
        results = sp.current_user_playlists(limit=50)
        while results and self.running:
            playlists.extend(results['items'])
            try:
                results = sp.next(results) if results['next'] else None
            except Exception as e:
                self.log('ERROR', f"Playlist pagination error: {str(e)}")
                break
        return playlists

    def get_playlist_tracks(self, sp, playlist_id):
        tracks = []
        results = sp.playlist_tracks(playlist_id)
        while results and self.running:
            tracks.extend([
                item['track'] for item in results['items']
                if item['track'] and not item['track']['is_local'] 
                and item['track'].get('is_playable', True)
            ])
            results = sp.next(results) if results['next'] else None
        return tracks

    def get_liked_tracks(self, sp):
        tracks = []
        results = sp.current_user_saved_tracks()
        while results and self.running:
            tracks.extend([
                item['track'] for item in results['items']
                if item['track'] and not item['track']['is_local'] 
                and item['track'].get('is_playable', True)
            ])
            results = sp.next(results) if results['next'] else None
        return tracks

    def get_cached_track(self, cache_dir, track_id):
        for ext in ['mp3', 'ogg', 'm4a']:
            cached_file = cache_dir / f"{track_id}.{ext}"
            if cached_file.exists():
                return cached_file
        return None

    def download_track(self, track_id, cache_dir):
        try:
            result = subprocess.run(
                ['spotdl', 'download', 
                 f"https://open.spotify.com/track/{track_id}",
                 '--output', f"{cache_dir}/{track_id}.{{output-ext}}"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                return next(cache_dir.glob(f"{track_id}.*"), None)
            return None
        except Exception as e:
            self.log('ERROR', f"Download failed {track_id}: {str(e)}")
            return None

    def embed_track_id(self, file_path, track_id):
        try:
            audio = ID3(file_path)
        except Exception:
            audio = ID3()
        audio.add(TXXX(encoding=3, desc='SpotifyID', text=track_id))
        audio.save(file_path)

    def load_song_map(self):
        if self.song_map_path.exists():
            try:
                return json.loads(self.song_map_path.read_text())
            except Exception:
                return {}
        return {}

    def save_song_map(self):
        self.song_map_path.write_text(json.dumps(self.song_map))

    def update_song_mapping(self, file_name, track_id):
        self.song_map[file_name] = track_id
        self.save_song_map()

    def configure_playlists(self, playlists, download_liked_default):
        config = {'selected': [], 'liked': False}
        print("\n=== Playlist Selection ===")
        print("Available playlists:")
        for idx, playlist in enumerate(playlists):
            print(f"{idx+1}. {playlist['name']} ({playlist['tracks']['total']} tracks)")
        
        selections = input("\nEnter playlist numbers to download (comma-separated, e.g. 1,3,5): ").strip()
        if selections:
            selected_indices = [int(idx.strip())-1 for idx in selections.split(',') if idx.strip().isdigit()]
            config['selected'] = [playlists[i]['id'] for i in selected_indices if i < len(playlists)]
        
        if download_liked_default:
            liked = input("\nDownload liked songs? (y/n): ").strip().lower()
            config['liked'] = liked == 'y'
        
        # New: allow entering multiple playlist URLs until 'start' is entered.
        while True:
            url_input = input("Enter a playlist URL to add or type 'start' to continue: ").strip()
            if url_input.lower() == 'start':
                break
            import re
            match = re.search(r'playlist/([a-zA-Z0-9]+)', url_input)
            if match:
                playlist_id = match.group(1)
                if playlist_id not in config['selected']:
                    config['selected'].append(playlist_id)
                    print(f"Added playlist with ID: {playlist_id}")
                else:
                    print("Playlist already added.")
            else:
                print("Invalid URL format, skipping playlist URL.")
        
        return config

    def log(self, level, message):
        colors = {
            'INFO': '\033[94m',    'SUCCESS': '\033[92m',
            'WARN': '\033[93m',    'ERROR': '\033[91m',
            'RESET': '\033[0m'
        }
        timestamp = time.strftime("%H:%M:%S")
        tqdm.write(f"[{timestamp}] {colors.get(level, '')}[{level}] {message}{colors['RESET']}")

    def process_playlist(self, sp, playlist, settings, main_pbar):
        if not self.running:
            return None

        playlist_id = 'liked' if playlist == 'liked' else playlist['id']
        state_key = f"playlist_{playlist_id}"
        processed_tracks = set(self.current_state.get(state_key, []))
        
        if playlist == 'liked':
            tracks = self.get_liked_tracks(sp)
            folder_name = "Liked_Songs"
        else:
            tracks = self.get_playlist_tracks(sp, playlist['id'])
            folder_name = f"{self.sanitize_name(playlist['name'])} [{playlist['id']}]"
        
        playlist_folder = Path(settings['root_dir']) / folder_name
        playlist_folder.mkdir(parents=True, exist_ok=True)
        cache_dir = Path(settings['cache_dir'])

        def process_track(track):
            if not track or not track.get('id'):
                return
            track_id = track['id']
            dest_file = playlist_folder / f"{self.sanitize_name(track['name'])}.mp3"
            if dest_file.exists() and self.song_map.get(dest_file.name) == track_id:
                return
            
            self.log('INFO', f"Downloading: {track['name']} [{track_id[:6]}]")
            cached_file = self.get_cached_track(cache_dir, track_id)
            if cached_file:
                shutil.copy(cached_file, dest_file)
            else:
                downloaded = self.download_track(track_id, cache_dir)
                if downloaded:
                    shutil.copy(downloaded, dest_file)
            
            if dest_file.exists():
                self.embed_track_id(dest_file, track_id)
                # New: Fetch and embed lyrics if available.
                if self.genius:
                    lyrics = self.fetch_lyrics(track)
                    if lyrics:
                        self.embed_lyrics(dest_file, lyrics)
                self.update_song_mapping(dest_file.name, track_id)
                processed_tracks.add(track_id)
                self.current_state[state_key] = list(processed_tracks)
                self.save_state()

        with concurrent.futures.ThreadPoolExecutor(max_workers=settings['parallel_downloads']) as executor:
            futures = [executor.submit(process_track, track) for track in tracks if track]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.log('ERROR', f"Track error: {str(e)}")
                main_pbar.update(int(70 / len(tracks)))

        current_ids = {t['id'] for t in tracks if t}
        for file in playlist_folder.glob('*.*'):
            file_id = self.song_map.get(file.name)
            if file_id not in current_ids:
                file.unlink()
                if file.name in self.song_map:
                    del self.song_map[file.name]
        self.save_song_map()
        return playlist_folder.name

    def run(self):
        settings = self.read_settings()
        sp = self.get_spotify_client(settings)
        self.current_state = self.load_state()
        self.song_map = self.load_song_map()
        # Initialize Genius client if token provided
        self.genius = lyricsgenius.Genius(settings['genius_token']) if settings['genius_token'] else None
        
        # Fetch playlists and configure without showing the progress bar.
        all_playlists = self.get_all_playlists(sp)
        self.log('INFO', f"Found {len(all_playlists)} playlists")
        
        config_path = Path("playlist_config.json")
        if config_path.exists():
            use_saved = input("Use saved playlist config? (y/n): ").strip().lower()
            if use_saved == 'y':
                config = json.loads(config_path.read_text())
            else:
                config = self.configure_playlists(all_playlists, settings['download_liked'])
        else:
            config = self.configure_playlists(all_playlists, settings['download_liked'])
        config_path.write_text(json.dumps(config))
        
        # Now display the progress bar only during the downloading process.
        try:
            with tqdm(total=100, desc="Downloading", position=0, leave=True) as main_pbar:
                self.main_pbar = main_pbar
                filtered_playlists = [p for p in all_playlists if p['id'] in config.get('selected', [])]
                total_items = len(filtered_playlists) + (1 if config.get('liked') else 0)
                
                for idx, playlist in enumerate(filtered_playlists):
                    if not self.running:
                        break
                    self.process_playlist(sp, playlist, settings, main_pbar)
                    main_pbar.update(int(70 / total_items))
                
                if config.get('liked') and self.running:
                    self.process_playlist(sp, 'liked', settings, main_pbar)
                    main_pbar.update(10)
                
                main_pbar.update(10)
                self.clear_state()
                self.log('SUCCESS', "Sync completed successfully")
        except Exception as e:
            self.log('ERROR', f"Fatal error: {str(e)}")
            self.save_state()
            sys.exit(1)

    def fetch_lyrics(self, track):
        """Fetch lyrics from Genius using track name and first artist."""
        if not self.genius:
            return None
        try:
            artist = track['artists'][0]['name'] if track.get('artists') else ''
            song = self.genius.search_song(track['name'], artist)
            return song.lyrics if song else None
        except Exception as e:
            self.log('ERROR', f"Error fetching lyrics for {track['name']}: {str(e)}")
            return None

    def embed_lyrics(self, file_path, lyrics):
        """Embed lyrics into the audio file's ID3 tag using USLT frame."""
        from mutagen.id3 import USLT
        try:
            audio = ID3(file_path)
        except Exception:
            audio = ID3()
        audio.delall("USLT")
        audio.add(USLT(encoding=3, lang="eng", desc="Lyrics", text=lyrics))
        audio.save(file_path)

if __name__ == "__main__":
    sync = SpotifySync()
    sync.run()