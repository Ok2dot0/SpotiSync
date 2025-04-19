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
import logging # Import logging
from pathlib import Path
from tqdm import tqdm

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from mutagen.id3 import ID3, TXXX
import concurrent.futures
import lyricsgenius  # new import for lyrics

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%H:%M:%S')

# --- Read essential credentials directly ---
config = configparser.ConfigParser()
settings_path = Path('settings.ini')
if not settings_path.exists():
    logging.error("settings.ini not found. Please create it with [DEFAULT] including CLIENT_ID, CLIENT_SECRET, REDIRECT_URI.")
    sys.exit(1)
config.read(settings_path)

try:
    CLIENT_ID = config['DEFAULT']['CLIENT_ID']
    CLIENT_SECRET = config['DEFAULT']['CLIENT_SECRET']
    REDIRECT_URI = config['DEFAULT']['REDIRECT_URI']
    GENIUS_TOKEN = config['DEFAULT'].get('GENIUS_TOKEN', '').strip()
except KeyError as e:
    logging.error(f"Missing required key in [DEFAULT] section of settings.ini: {e}")
    sys.exit(1)
# --- End credential reading ---

class SpotifySync:
    def __init__(self):
        self.running = True
        self.paused = False
        self.state_file = Path('.sync_state.json')
        self.current_state = {}
        self.lock = threading.Lock()
        self.song_map_path = Path("song_mapping.json")
        self.song_map = {}
        self.total_tracks_to_process = 0 # Track total for progress bar
        # --- Hardcoded settings ---
        self.root_dir = 'Spotify Playlists'
        self.cache_dir = 'spotify_cache'
        self.parallel_downloads = 2
        # --- End Hardcoded settings ---
        self.genius = None # Initialize genius client later if token exists

    def signal_handler(self, sig, frame):
        logging.warning("Ctrl+C detected. Stopping gracefully...")
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

    def get_spotify_client(self):
        # Use credentials read at the start
        auth_manager = SpotifyOAuth(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            redirect_uri=REDIRECT_URI,
            scope='playlist-read-private playlist-read-collaborative user-library-read'
        )
        return spotipy.Spotify(auth_manager=auth_manager)

    def get_all_playlists(self, sp):
        playlists = []
        try:
            results = sp.current_user_playlists(limit=50)
            while results and self.running:
                playlists.extend(results['items'])
                results = sp.next(results) if results['next'] else None
        except Exception as e:
            logging.error(f"Error fetching playlists: {str(e)}")
            # Decide if execution should stop or continue with potentially partial list
            # For now, log and continue
        return playlists

    def get_playlist_tracks(self, sp, playlist_id):
        tracks = []
        try:
            results = sp.playlist_tracks(playlist_id)
            while results and self.running:
                tracks.extend([
                    item['track'] for item in results['items']
                    if item and item.get('track') and not item['track'].get('is_local', False)
                    # Check if 'is_playable' exists before accessing it
                    and item['track'].get('is_playable', True) 
                ])
                results = sp.next(results) if results['next'] else None
        except Exception as e:
             logging.error(f"Error fetching tracks for playlist {playlist_id}: {str(e)}")
             # Return empty list on error
             return []
        return tracks

    def get_liked_tracks(self, sp):
        tracks = []
        try:
            results = sp.current_user_saved_tracks()
            while results and self.running:
                tracks.extend([
                    item['track'] for item in results['items']
                    if item and item.get('track') and not item['track'].get('is_local', False)
                    # Check if 'is_playable' exists before accessing it
                    and item['track'].get('is_playable', True)
                ])
                results = sp.next(results) if results['next'] else None
        except Exception as e:
            logging.error(f"Error fetching liked tracks: {str(e)}")
            # Return empty list on error
            return []
        return tracks

    def get_cached_track(self, cache_dir, track_id):
        for ext in ['mp3', 'ogg', 'm4a']:
            cached_file = cache_dir / f"{track_id}.{ext}"
            if cached_file.exists():
                return cached_file
        return None

    def download_track(self, track_id, cache_dir):
        try:
            # Ensure cache_dir exists
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            result = subprocess.run(
                ['spotdl', 'download',
                 f"https://open.spotify.com/track/{track_id}",
                 '--output', f"{cache_dir}/{track_id}.{{output-ext}}"],
                capture_output=True,
                text=True,
                check=False # Don't raise exception on non-zero exit
            )
            if result.returncode == 0:
                # Find the downloaded file, as extension might vary
                found_files = list(cache_dir.glob(f"{track_id}.*"))
                if found_files:
                    logging.info(f"Downloaded: {track_id}")
                    return found_files[0]
                else:
                    logging.warning(f"spotdl reported success but no file found for {track_id}")
                    return None
            else:
                # Log spotdl's stderr for debugging
                logging.error(f"spotdl failed for {track_id}. Error: {result.stderr.strip()}")
                return None
        except FileNotFoundError:
             logging.error("spotdl command not found. Make sure it's installed and in PATH.")
             self.running = False # Stop processing if spotdl isn't found
             return None
        except Exception as e:
            logging.error(f"Download exception for {track_id}: {str(e)}")
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

    def configure_playlists(self, playlists): # Removed download_liked_default parameter
        config = {'selected': [], 'liked': False}
        print("\n=== Playlist Selection ===")
        if not playlists:
            print("No playlists found for your account.")
        else:
            print("Available playlists:")
            for idx, playlist in enumerate(playlists):
                print(f"{idx+1}. {playlist['name']} ({playlist.get('tracks', {}).get('total', 0)} tracks)")
            
            while True:
                selections = input("\nEnter playlist numbers to download (comma-separated, e.g. 1,3,5), or leave blank: ").strip()
                if not selections:
                    break
                try:
                    selected_indices = [int(idx.strip())-1 for idx in selections.split(',') if idx.strip().isdigit()]
                    valid_indices = [i for i in selected_indices if 0 <= i < len(playlists)]
                    if len(valid_indices) != len(selected_indices):
                        logging.warning("Some invalid playlist numbers were entered.")
                    config['selected'] = list(set(config['selected'] + [playlists[i]['id'] for i in valid_indices])) # Use set to avoid duplicates
                    break # Exit loop after successful selection
                except ValueError:
                    print("Invalid input. Please enter numbers separated by commas.")

        # Ask about liked songs directly
        while True:
            liked = input("\nDownload liked songs? (y/n): ").strip().lower()
            if liked in ['y', 'n']:
                config['liked'] = liked == 'y'
                break
            else:
                print("Invalid input. Please enter 'y' or 'n'.")
        
        # Add playlists by URL
        while True:
            url_input = input("Enter a playlist URL to add (or type 'start' to continue): ").strip()
            if url_input.lower() == 'start':
                break
            
            match = re.search(r'playlist/([a-zA-Z0-9]+)', url_input)
            if match:
                playlist_id = match.group(1)
                if playlist_id not in config['selected']:
                    # Optional: Verify playlist exists via API before adding?
                    config['selected'].append(playlist_id)
                    logging.info(f"Added playlist by URL: {playlist_id}")
                else:
                    logging.warning("Playlist from URL already selected.")
            else:
                logging.warning("Invalid Spotify playlist URL format.")
        
        return config

    def process_playlist(self, sp, playlist_data, pbar): # Removed settings parameter
        """Processes a single playlist or liked songs."""
        if not self.running:
            return None

        is_liked = playlist_data == 'liked'
        playlist_id = 'liked' if is_liked else playlist_data['id']
        state_key = f"playlist_{playlist_id}"
        # Load processed tracks for this specific playlist from state
        processed_tracks_in_state = set(self.current_state.get(state_key, []))
        
        tracks = []
        folder_name = ""
        
        if is_liked:
            logging.info("Fetching liked songs...")
            tracks = self.get_liked_tracks(sp)
            folder_name = "Liked_Songs"
        else:
            logging.info(f"Fetching tracks for playlist: {playlist_data['name']} ({playlist_id})")
            tracks = self.get_playlist_tracks(sp, playlist_id)
            folder_name = f"{self.sanitize_name(playlist_data['name'])} [{playlist_id}]"
        
        if not tracks:
             logging.warning(f"No tracks found or error fetching for {folder_name}. Skipping.")
             return folder_name # Return folder name even if empty for cleanup logic

        # Use hardcoded root_dir and cache_dir
        playlist_folder = Path(self.root_dir) / folder_name
        playlist_folder.mkdir(parents=True, exist_ok=True)
        cache_dir = Path(self.cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True) # Ensure cache dir exists

        # --- Track Processing Logic ---
        tracks_to_process_this_run = []
        current_track_ids_in_playlist = set()

        for track in tracks:
            if not track or not track.get('id'):
                logging.warning("Skipping invalid track data.")
                continue
            
            track_id = track['id']
            current_track_ids_in_playlist.add(track_id)
            sanitized_track_name = self.sanitize_name(track['name'])
            
            # Determine expected filename (spotdl might use different extensions)
            # We'll check existence later more robustly
            
            # Check if track needs processing (exists in folder with correct ID?)
            needs_processing = True
            potential_files = list(playlist_folder.glob(f"{re.escape(sanitized_track_name)}.*"))
            for existing_file in potential_files:
                 # Check embedded ID if file exists
                 try:
                     audio = ID3(existing_file)
                     spotify_id_tags = audio.getall('TXXX:SpotifyID')
                     if spotify_id_tags and spotify_id_tags[0].text[0] == track_id:
                         needs_processing = False
                         break # Found matching file
                 except Exception:
                     # Error reading tags, assume needs processing
                     logging.warning(f"Could not read ID3 tags for {existing_file.name}, will re-process if necessary.")
                     pass
            
            if needs_processing:
                tracks_to_process_this_run.append(track)
            # else: # Optionally log skips
            #     logging.info(f"Skipping already present: {sanitized_track_name}")

        logging.info(f"Playlist '{folder_name}': {len(tracks_to_process_this_run)} tracks to process.")

        def process_single_track(track_info):
            """Downloads/copies and embeds info for a single track."""
            if not self.running: return False
            
            track_id = track_info['id']
            sanitized_name = self.sanitize_name(track_info['name'])
            # Use self.cache_dir
            cache_dir_path = Path(self.cache_dir) 
            
            # Try finding in cache first
            cached_file = self.get_cached_track(cache_dir_path, track_id)
            
            dest_file_path = None # Initialize dest_file_path

            if cached_file:
                logging.info(f"Using cache for: {sanitized_name} [{track_id[:6]}]")
                target_path = playlist_folder / cached_file.name # Keep original extension
                try:
                    # Ensure target directory exists (redundant but safe)
                    playlist_folder.mkdir(parents=True, exist_ok=True)
                    shutil.copy(cached_file, target_path)
                    dest_file_path = target_path
                except Exception as e:
                    logging.error(f"Failed to copy {cached_file.name} to {playlist_folder}: {e}")
                    return False # Failed copy
            else:
                logging.info(f"Downloading: {sanitized_name} [{track_id[:6]}]")
                # Pass self.cache_dir path object
                downloaded_file_path = self.download_track(track_id, cache_dir_path) 
                if downloaded_file_path:
                    target_path = playlist_folder / downloaded_file_path.name
                    try:
                        # Ensure target directory exists (redundant but safe)
                        playlist_folder.mkdir(parents=True, exist_ok=True)
                        # Move from cache if download was successful
                        shutil.move(downloaded_file_path, target_path)
                        dest_file_path = target_path
                    except Exception as e:
                        logging.error(f"Failed to move {downloaded_file_path.name} to {playlist_folder}: {e}")
                        # Try copying as fallback? Or just fail? For now, fail.
                        return False
                else:
                    # Download failed
                    return False

            # Embed Spotify ID and Lyrics if download/copy was successful
            if dest_file_path and dest_file_path.exists():
                try:
                    self.embed_track_id(dest_file_path, track_id)
                    if self.genius: # Check if genius client is initialized
                        lyrics = self.fetch_lyrics(track_info)
                        if lyrics:
                            self.embed_lyrics(dest_file_path, lyrics)
                            logging.info(f"Lyrics embedded for: {sanitized_name}")
                    
                    # Update song map (using the final filename with extension)
                    self.update_song_mapping(dest_file_path.name, track_id)
                    return True # Success
                except Exception as e:
                    logging.error(f"Failed to embed metadata for {dest_file_path.name}: {e}")
                    return False # Embedding failed
            return False # File doesn't exist after process

        # Process tracks using ThreadPoolExecutor with hardcoded parallel_downloads
        successful_processed_ids = set()
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.parallel_downloads) as executor:
            future_to_track = {executor.submit(process_single_track, track): track for track in tracks_to_process_this_run}
            for future in concurrent.futures.as_completed(future_to_track):
                track = future_to_track[future]
                try:
                    success = future.result()
                    if success:
                        successful_processed_ids.add(track['id'])
                except Exception as e:
                    logging.error(f"Error processing track {track.get('name', 'N/A')}: {e}")
                finally:
                     pbar.update(1) # Update main progress bar by 1 for each track attempted

        # Update state with successfully processed tracks for this playlist
        with self.lock:
            # Combine state: already processed + newly processed successfully
            updated_processed_tracks = processed_tracks_in_state.union(successful_processed_ids)
            self.current_state[state_key] = list(updated_processed_tracks)
            self.save_state() # Save state after each playlist batch

        # --- Cleanup Logic ---
        logging.info(f"Cleaning up folder: {playlist_folder.name}")
        files_in_folder = list(playlist_folder.glob('*.*')) # Get all files once
        ids_in_folder_map = {file.name: self.song_map.get(file.name) for file in files_in_folder if file.name in self.song_map}
        
        files_to_remove = []
        for file_name, file_track_id in ids_in_folder_map.items():
            if file_track_id not in current_track_ids_in_playlist:
                files_to_remove.append(playlist_folder / file_name)

        if files_to_remove:
            logging.info(f"Removing {len(files_to_remove)} obsolete tracks from {playlist_folder.name}")
            for file_to_remove in files_to_remove:
                try:
                    file_to_remove.unlink()
                    # Remove from song_map as well
                    if file_to_remove.name in self.song_map:
                        del self.song_map[file_to_remove.name]
                except Exception as e:
                    logging.error(f"Failed to remove {file_to_remove.name}: {e}")
        
        # Save song map after cleanup
        self.save_song_map()
        
        return folder_name # Indicate playlist processed

    def run(self):
        # Register signal handler early
        signal.signal(signal.SIGINT, self.signal_handler)
        
        try:
            sp = self.get_spotify_client() # Removed settings parameter
            # Test connection early
            sp.current_user() 
            logging.info("Successfully connected to Spotify.")
        except Exception as e:
            logging.error(f"Failed to connect to Spotify: {e}")
            logging.error("Please check your credentials and network connection.")
            sys.exit(1)
            
        self.current_state = self.load_state()
        self.song_map = self.load_song_map()
        
        # Initialize Genius client using token read at start
        if GENIUS_TOKEN:
            logging.info("Genius token found, enabling lyrics fetching.")
            self.genius = lyricsgenius.Genius(GENIUS_TOKEN, verbose=False, remove_section_headers=True, skip_non_songs=True, excluded_terms=["(Remix)", "(Live)"])
        else:
            logging.warning("No Genius token found in settings.ini. Lyrics will not be fetched.")
            self.genius = None
        
        # --- Configuration Phase ---
        logging.info("Fetching user playlists...")
        all_playlists = self.get_all_playlists(sp)
        logging.info(f"Found {len(all_playlists)} playlists.")
        
        config_path = Path("playlist_config.json")
        config = {}
        if config_path.exists():
            try:
                saved_config = json.loads(config_path.read_text())
                while True:
                    use_saved = input(f"Found saved config ({len(saved_config.get('selected',[]))} playlists, Liked: {saved_config.get('liked', False)}). Use it? (y/n): ").strip().lower()
                    if use_saved == 'y':
                        config = saved_config
                        logging.info("Using saved playlist configuration.")
                        break
                    elif use_saved == 'n':
                        config = self.configure_playlists(all_playlists) # Removed settings parameter
                        config_path.write_text(json.dumps(config)) # Save new config
                        logging.info("Saved new playlist configuration.")
                        break
                    else:
                        print("Invalid input. Please enter 'y' or 'n'.")
            except Exception as e:
                 logging.error(f"Error loading saved config: {e}. Re-configuring.")
                 config = self.configure_playlists(all_playlists) # Removed settings parameter
                 config_path.write_text(json.dumps(config)) # Save new config
        else:
            config = self.configure_playlists(all_playlists) # Removed settings parameter
            config_path.write_text(json.dumps(config)) # Save new config
            logging.info("Saved new playlist configuration.")
        
        # --- Preparation Phase ---
        playlists_to_process = [p for p in all_playlists if p['id'] in config.get('selected', [])]
        items_to_process = playlists_to_process + (['liked'] if config.get('liked') else [])
        
        if not items_to_process:
            logging.warning("No playlists or liked songs selected for download. Exiting.")
            sys.exit(0)

        # Calculate total tracks for progress bar *before* starting downloads
        logging.info("Calculating total tracks to process...")
        self.total_tracks_to_process = 0
        temp_tracks_cache = {} # Cache track lists to avoid re-fetching

        # Use a temporary progress bar for calculation
        with tqdm(total=len(items_to_process), desc="Calculating total", unit="playlist", leave=False) as calc_pbar:
            for item in items_to_process:
                if not self.running: break
                if item == 'liked':
                    tracks = self.get_liked_tracks(sp)
                    temp_tracks_cache['liked'] = tracks
                    self.total_tracks_to_process += len(tracks)
                else:
                    playlist_id = item['id']
                    tracks = self.get_playlist_tracks(sp, playlist_id)
                    temp_tracks_cache[playlist_id] = tracks
                    self.total_tracks_to_process += len(tracks)
                calc_pbar.update(1)
        
        if not self.running:
             logging.warning("Operation cancelled during calculation.")
             sys.exit(0)
             
        logging.info(f"Total tracks across selected items: {self.total_tracks_to_process}")

        # --- Processing Phase ---
        try:
            # Use the calculated total for the main progress bar
            with tqdm(total=self.total_tracks_to_process, desc="Syncing", unit="track", position=0, leave=True) as main_pbar:
                for item_data in items_to_process:
                    if not self.running:
                        break
                    # Pass the main_pbar to process_playlist
                    self.process_playlist(sp, item_data, main_pbar) # Removed settings parameter
                    # No need to update main_pbar here, it's updated per track inside process_playlist

                if self.running:
                    # Ensure progress bar completes if all tracks were processed
                    # It might be slightly off if errors occurred, but this ensures it reaches 100% visually on success.
                    main_pbar.n = self.total_tracks_to_process
                    main_pbar.refresh()
                    logging.info("Sync process finished.")
                else:
                    logging.warning("Sync process was interrupted.")

        except Exception as e:
            logging.exception(f"An unexpected error occurred during the sync process: {e}") # Log full traceback
            self.save_state() # Save state on unexpected error
            sys.exit(1)
        finally:
            # Cleanup state file only on successful completion without interruption
            if self.running:
                self.clear_state()
                logging.info("Cleared sync state file.")
            else:
                 logging.warning("Sync interrupted, state file preserved.")

    def fetch_lyrics(self, track):
        """Fetch lyrics from Genius using track name and first artist."""
        if not self.genius:
            return None
        try:
            # Try to get artist name safely
            artist_name = track.get('artists', [{}])[0].get('name', '')
            if not artist_name:
                 logging.warning(f"No artist found for track {track.get('name', 'N/A')}, skipping lyrics.")
                 return None
                 
            logging.info(f"Fetching lyrics for: {track['name']} by {artist_name}")
            song = self.genius.search_song(track['name'], artist_name)
            if song and song.lyrics:
                 # Basic cleanup of lyrics
                 lyrics = song.lyrics.strip()
                 # Remove the first line if it's just the song title and 'Lyrics'
                 lines = lyrics.split('\n')
                 if len(lines) > 1 and f"{track['name']} Lyrics".lower() in lines[0].lower():
                     lyrics = '\n'.join(lines[1:]).strip()
                 # Remove embed text
                 lyrics = re.sub(r'\d*EmbedShare URLCopyEmbedCopy', '', lyrics).strip()
                 return lyrics
            else:
                 logging.warning(f"No lyrics found for: {track['name']}")
                 return None
        except Exception as e:
            # Catch potential errors during Genius search (e.g., network issues, timeouts)
            logging.error(f"Error fetching lyrics for {track.get('name', 'N/A')}: {str(e)}")
            return None

    def embed_lyrics(self, file_path, lyrics):
        """Embed lyrics into the audio file's ID3 tag using USLT frame."""
        from mutagen.id3 import USLT, ID3NoHeaderError
        try:
            audio = ID3(file_path)
        except ID3NoHeaderError:
            # File might not have ID3 tags yet, create them
            logging.warning(f"No ID3 header found in {file_path.name}, creating one.")
            audio = ID3()
        except Exception as e:
            logging.error(f"Error reading ID3 tags for {file_path.name}: {e}")
            return # Cannot embed if tags can't be read/created

        try:
            # Remove existing lyrics tags before adding new one
            audio.delall("USLT")
            # Add the lyrics tag (encoding=3 is UTF-8)
            audio.add(USLT(encoding=3, lang='eng', desc='Lyrics', text=lyrics))
            audio.save(v2_version=3) # Save using a common ID3v2 version
        except Exception as e:
            logging.error(f"Error embedding lyrics into {file_path.name}: {e}")


if __name__ == "__main__":
    # Ensure necessary directories exist before starting using hardcoded paths
    try:
        # Use hardcoded paths directly
        root = 'Spotify Playlists'
        cache = 'spotify_cache'
        Path(root).mkdir(parents=True, exist_ok=True)
        Path(cache).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logging.error(f"Error preparing directories: {e}")
        # Allow script to continue and potentially fail later if dirs are crucial

    sync = SpotifySync()
    sync.run()