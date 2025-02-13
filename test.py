import sys
import spotipy
import spotipy.util as util

username='Ok 2.0'
clientid='d1f252dad38f48e28af70e7a0758b9c8'
clientsecret='6defc4437e2d4d0aae127ee685c3509f'
redirecturi='http://localhost'
thescope='http://localhost:8080'

token = util.prompt_for_user_token(username,scope=thescope,client_id=clientid,client_secret=clientsecret,redirect_uri=redirecturi)
if token:
    sp = spotipy.Spotify(auth=token)
    playlists = sp.current_user_playlists()
    while playlists:
        for i, playlist in enumerate(playlists['items']):
            print("%4d %s %s" % (i + 1 + playlists['offset'], playlist['uri'],  playlist['name']))
        if playlists['next']:
            print("getting next 50")
            playlists = sp.next(playlists)
        else:
            playlists = None
else:
    print ("Can't get token for", username)