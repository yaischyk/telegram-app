import asyncio
import time
from pymongo import MongoClient
from telethon import TelegramClient, events, types

cluster = MongoClient('mongodb+srv://admin:<password>@cluster0.hrd7w1m.mongodb.net')
db = cluster.project

async def account(session):
    account = 'sessions/events/' + str(session['_id'])
    api_id = session['api_id']
    api_hash = session['api_hash']

    client = TelegramClient(account, api_id, api_hash)

    # Get chats
    chats = [int('-100' + str(x['channel_id'])) for x in db.channels.find({'session': session['_id'], 'channel_id': {'$ne': None}})]
    #print(chats)
    
    @client.on(events.NewMessage(chats=chats))
    async def new(event):
        message = event.message

        # Forwarded Message
        fwd_from = None
        if isinstance(message.fwd_from, types.MessageFwdHeader):
            if isinstance(message.fwd_from.from_id, types.PeerChannel):
                fwd_from = {'channel_id': message.fwd_from.from_id.channel_id}

        # Media
        media = []

        # Photo
        if isinstance(message.media, types.MessageMediaPhoto):
            download_media = await client.download_media(message.media, 'media/photos/' + str(message.media.photo.id))
            media.append({'type': 'photo', 'name': download_media.split('\\')[-1]})

        # Document
        if isinstance(message.media, types.MessageMediaDocument):
            if message.media.document.size < 2000000:
                download_media = await client.download_media(message.media, 'media/documents/' + str(message.media.document.id))
                media.append({'type': 'document', 'name': download_media.split('\\')[-1]})
            else:
                media.append({'type': 'document', 'name': None, 'size': message.media.document.size})

        if not media:
            media = None

        # Mentioned
        links = []
        # Get links with entities
        if message.entities is not None:
            for x in message.entities:
                if isinstance(x, types.MessageEntityTextUrl):
                    if 'url' in dir(x):
                        if 't.me' in x.url:
                            links.append(x.url)

        # Get links with message
        text = message.message
        split_text = text.split()
        keys = ['@', 't.me']

        for i in range(len(split_text)):
            for key in keys:
                if key in split_text[i]:
                    links.append(split_text[i])

        mentioned = []
        for link in links:
            split_link = link.split('/')
            first_sign = split_link[-1][0]

            if first_sign == '+' or first_sign == '@':
                peer = split_link[-1][1:]
            else:
                peer = split_link[-1]

            if '+' in link or 'joinchat' in link:
                peer_type = 'hash'
            else:
                peer_type = 'username'

            # Get Mentioned
            mentioned_id = None
            get_mentioned = db.peers.find_one({peer_type: peer})
            if get_mentioned:
                mentioned_id = get_mentioned['channel_id']

            if not get_mentioned:
                if not db.channels.find_one({peer_type: peer}):
                    db.channels.insert_one({peer_type: peer})

            if mentioned_id != message.peer_id.channel_id:
                mentioned.append({peer_type: peer, 'channel_id': mentioned_id})

        if not mentioned:
            mentioned = None

        publication = {
            'post_id': message.id,
            'peer_id': {'channel_id': message.peer_id.channel_id},
            'mentioned': mentioned,
            'date': message.date,
            'message': message.message,
            'fwd_from': fwd_from,
            'media': media,
            'grouped_id': message.grouped_id
        }

        db.publications.insert_one(publication)

    await client.start()
    await client.set_receive_updates(True)
    await client.run_until_disconnected()

async def main():
    sessions = []
    for session in db.sessions.find():
        sessions.append(account(session))

    await asyncio.gather(*sessions)

if __name__ == '__main__':
    # Запускаем цикл событий
    asyncio.run(main())
