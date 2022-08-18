import asyncio
import time
import datetime
from telethon import TelegramClient, functions, types, errors
from pymongo import MongoClient

cluster = MongoClient('mongodb+srv://admin:<password>@cluster0.hrd7w1m.mongodb.net')
db = cluster.project

async def account(session=None):
    print('Runnig account... Please wait... ' + str(session['_id']))
    account = 'sessions/clients/' + str(session['_id'])
    api_id = session['api_id']
    api_hash = session['api_hash']
    

    async with TelegramClient(account, api_id, api_hash) as client:
        print('Run: ' + str(session['_id']))
        # Get Controller
        async def get():
            for data in db.channels.find({'channel_id': None, 'session': session['_id']}):
                try:
                    join = False

                    if 'username' in data:
                        peer_type = 'username'
                        query_peer = data['username']
                        peer = data['username']

                        # Join Chat
                        try:
                            await client(functions.channels.JoinChannelRequest(channel=peer))
                            join = True
                        except Exception as e:
                            print(e)

                    elif 'hash' in data:
                        peer_type = 'hash'
                        query_peer = 't.me/joinchat/' + data['hash']
                        peer = data['hash']
                        # Import Chat Invite
                        try:
                            await client(functions.messages.ImportChatInviteRequest(hash=peer))
                        except Exception as e:
                            if isinstance(e, errors.UserAlreadyParticipantError):
                                join = True
                                if isinstance(e, errors.InviteRequestSentError):
                                    if 'You have successfully requested' in str(e):
                                        join = True
                                if isinstance(e, errors.FloodWaitError):
                                    print(e)
                                    
                    if join:
                        try:
                            result = await client(functions.channels.GetFullChannelRequest(query_peer))
                            result = result.to_dict()
                            channel_id = result['chats'][0]['id']

                            # Duplicate
                            duplicate = db.peers.find_one({'channel_id': channel_id})
                            if duplicate:
                                if 'username' in duplicate:
                                    duplicate_peer_type = 'username'
                                else:
                                    duplicate_peer_type = 'hash'

                                if duplicate[duplicate_peer_type] != peer:
                                    # Update peer
                                    db.channels.update_one({'channel_id': channel_id}, {'$set': {peer_type: peer}})
                                    # Add to Peers
                                    db.peers.insert_one({'channel_id': channel_id, peer_type: peer, 'date': datetime.datetime.utcnow()})
                                    # Delete peer
                                    db.channels.delete_one({'_id': data['_id']})
                            else:
                                db.peers.insert_one({'channel_id': channel_id, peer_type: peer, 'date': datetime.datetime.utcnow()})

                            # Insert Chat
                            channel = {
                                "$set": {
                                    "channel_id": channel_id,
                                    "title": result['chats'][0]['title'],
                                    "participants_count": result['full_chat']['participants_count'],
                                    "about": result['full_chat']['about'],
                                    "verified": result['chats'][0]['verified'],
                                    "scam": result['chats'][0]['scam']
                                }
                            }

                            # Insert query
                            db.channels.update_one({"_id": data['_id']}, channel)

                            # Update posts
                            db.publications.update_many({"mentioned." + peer_type: peer}, {"$set": {"mentioned.$.channel_id": channel_id}})
                        except Exception as e:
                            print(e)
                    else:
                        # Delete Peer (It's not Channel)
                        # db.channels.delete_one({peer_type: peer})
                        print('delete')
                except Exception as e:
                    print(e)

        # Update Controller
        async def update():
            print('update')

        # Participants Controller
        async def participants():
            for data in db.channels.find({'channel_id': {'$ne': None}, 'session': session['_id']}):
                channel_id = data['channel_id']
                try:
                    participants_count = await client.get_participants(channel_id, limit=0)
                    total = participants_count.total

                    db.growth.insert_one({'date': datetime.datetime.utcnow(), 'peer': {'channel_id': channel_id}, 'participants_count': total})
                except Exception as e:
                    print(e)

        # Views Controller
        async def views():
            print('view')

        # Join Function
        async def join():
            print('join')
        
        # Schedule
        schedule = {'participants': False, 'get': False}
        while True:
            utc = datetime.datetime.utcnow()

            # Participants
            if (utc.minute == 0 and schedule['participants'] == False):
                await participants()
                schedule['participants'] = True
            
            if (utc.minute == 1):
                schedule['participants'] = False

            # Get
            if (utc.minute == 30 and schedule['get'] == False):
                await get()
                schedule['get'] = True
            
            if (utc.minute == 31):
                schedule['get'] = False

            await asyncio.sleep(1)

async def main():
    sessions = []
    for session in db.sessions.find():
        sessions.append(account(session=session))
    await asyncio.gather(*sessions)

if __name__ == '__main__':
    asyncio.run(main())
