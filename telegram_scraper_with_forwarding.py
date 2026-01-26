import os
import sqlite3
import json
import csv
import asyncio
import time
import sys
import uuid
import warnings
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from pathlib import Path
from io import StringIO
from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage, User, PeerChannel, Channel, Chat
from telethon.errors import FloodWaitError, SessionPasswordNeededError
import qrcode

warnings.filterwarnings("ignore", message="Using async sessions support is an experimental feature")

def display_ascii_art():
    WHITE = "\033[97m"
    RESET = "\033[0m"
    art = r"""
___________________  _________
\__    ___/  _____/ /   _____/
  |    | /   \  ___ \_____  \ 
  |    | \    \_\  \/        \
  |____|  \______  /_______  /
                 \/        \/
    """
    print(WHITE + art + RESET)

@dataclass
class MessageData:
    message_id: int
    date: str
    sender_id: int
    first_name: Optional[str]
    last_name: Optional[str]
    username: Optional[str]
    message: str
    media_type: Optional[str]
    media_path: Optional[str]
    reply_to: Optional[int]
    post_author: Optional[str]
    views: Optional[int]
    forwards: Optional[int]
    reactions: Optional[str]

@dataclass
class ForwardingRule:
    source_channel: str
    destination_channel: str
    forward_text: bool = True
    forward_images: bool = True
    forward_videos: bool = True
    forward_documents: bool = True
    forward_mode: str = "copy"
    enabled: bool = True

class OptimizedTelegramScraper:
    def __init__(self):
        self.STATE_FILE = 'state.json'
        self.state = self.load_state()
        self.client = None
        self.continuous_scraping_active = False
        self.forwarding_active = False
        self.max_concurrent_downloads = 5
        self.batch_size = 100
        self.state_save_interval = 50
        self.db_connections = {}
        self.forwarding_handler = None
        
    def load_state(self) -> Dict[str, Any]:
        if os.path.exists(self.STATE_FILE):
            try:
                with open(self.STATE_FILE, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {
            'api_id': None,
            'api_hash': None,
            'channels': {},
            'channel_names': {},
            'scrape_media': True,
            'forwarding_rules': [],
        }

    def save_state(self):
        try:
            with open(self.STATE_FILE, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            print(f"Failed to save state: {e}")

    def get_forwarding_rules(self) -> List[ForwardingRule]:
        rules = []
        for rule_dict in self.state.get('forwarding_rules', []):
            rules.append(ForwardingRule(
                source_channel=rule_dict['source_channel'],
                destination_channel=rule_dict['destination_channel'],
                forward_text=rule_dict.get('forward_text', True),
                forward_images=rule_dict.get('forward_images', True),
                forward_videos=rule_dict.get('forward_videos', True),
                forward_documents=rule_dict.get('forward_documents', True),
                forward_mode=rule_dict.get('forward_mode', 'copy'),
                enabled=rule_dict.get('enabled', True)
            ))
        return rules

    def save_forwarding_rule(self, rule: ForwardingRule):
        rule_dict = {
            'source_channel': rule.source_channel,
            'destination_channel': rule.destination_channel,
            'forward_text': rule.forward_text,
            'forward_images': rule.forward_images,
            'forward_videos': rule.forward_videos,
            'forward_documents': rule.forward_documents,
            'forward_mode': rule.forward_mode,
            'enabled': rule.enabled
        }
        
        existing_idx = None
        for i, existing in enumerate(self.state.get('forwarding_rules', [])):
            if (existing['source_channel'] == rule.source_channel and 
                existing['destination_channel'] == rule.destination_channel):
                existing_idx = i
                break
        
        if existing_idx is not None:
            self.state['forwarding_rules'][existing_idx] = rule_dict
        else:
            if 'forwarding_rules' not in self.state:
                self.state['forwarding_rules'] = []
            self.state['forwarding_rules'].append(rule_dict)
        
        self.save_state()

    def remove_forwarding_rule(self, index: int) -> bool:
        if 0 <= index < len(self.state.get('forwarding_rules', [])):
            del self.state['forwarding_rules'][index]
            self.save_state()
            return True
        return False

    def get_db_connection(self, channel: str) -> sqlite3.Connection:
        if channel not in self.db_connections:
            channel_dir = Path(channel)
            channel_dir.mkdir(exist_ok=True)

            db_file = channel_dir / f'{channel}.db'
            conn = sqlite3.connect(str(db_file), check_same_thread=False, timeout=30)
            conn.execute('''CREATE TABLE IF NOT EXISTS messages
                          (id INTEGER PRIMARY KEY, message_id INTEGER UNIQUE, date TEXT,
                           sender_id INTEGER, first_name TEXT, last_name TEXT, username TEXT,
                           message TEXT, media_type TEXT, media_path TEXT, reply_to INTEGER,
                           post_author TEXT, views INTEGER, forwards INTEGER, reactions TEXT)''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_message_id ON messages(message_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON messages(date)')
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            conn.commit()

            self.migrate_database(conn)

            self.db_connections[channel] = conn

        return self.db_connections[channel]

    def migrate_database(self, conn: sqlite3.Connection):
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(messages)")
        columns = {row[1] for row in cursor.fetchall()}

        migrations = []
        if 'post_author' not in columns:
            migrations.append('ALTER TABLE messages ADD COLUMN post_author TEXT')
        if 'views' not in columns:
            migrations.append('ALTER TABLE messages ADD COLUMN views INTEGER')
        if 'forwards' not in columns:
            migrations.append('ALTER TABLE messages ADD COLUMN forwards INTEGER')
        if 'reactions' not in columns:
            migrations.append('ALTER TABLE messages ADD COLUMN reactions TEXT')

        for migration in migrations:
            try:
                conn.execute(migration)
            except:
                pass

        if migrations:
            conn.commit()

    def close_db_connections(self):
        for conn in self.db_connections.values():
            conn.close()
        self.db_connections.clear()

    def batch_insert_messages(self, channel: str, messages: List[MessageData]):
        if not messages:
            return

        conn = self.get_db_connection(channel)
        data = [(msg.message_id, msg.date, msg.sender_id, msg.first_name,
                msg.last_name, msg.username, msg.message, msg.media_type,
                msg.media_path, msg.reply_to, msg.post_author, msg.views,
                msg.forwards, msg.reactions) for msg in messages]

        conn.executemany('''INSERT OR IGNORE INTO messages
                           (message_id, date, sender_id, first_name, last_name, username,
                            message, media_type, media_path, reply_to, post_author, views,
                            forwards, reactions)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)
        conn.commit()

    async def download_media(self, channel: str, message) -> Optional[str]:
        if not message.media or not self.state['scrape_media']:
            return None

        if isinstance(message.media, MessageMediaWebPage):
            return None

        try:
            channel_dir = Path(channel)
            media_folder = channel_dir / 'media'
            media_folder.mkdir(exist_ok=True)
            
            if isinstance(message.media, MessageMediaPhoto):
                original_name = getattr(message.file, 'name', None) or "photo.jpg"
                ext = "jpg"
            elif isinstance(message.media, MessageMediaDocument):
                ext = getattr(message.file, 'ext', 'bin') if message.file else 'bin'
                original_name = getattr(message.file, 'name', None) or f"document.{ext}"
            else:
                return None
            
            base_name = Path(original_name).stem
            extension = Path(original_name).suffix or f".{ext}"
            unique_filename = f"{message.id}-{base_name}{extension}"
            media_path = media_folder / unique_filename
            
            existing_files = list(media_folder.glob(f"{message.id}-*"))
            if existing_files:
                return str(existing_files[0])

            for attempt in range(3):
                try:
                    downloaded_path = await message.download_media(file=str(media_path))
                    if downloaded_path and Path(downloaded_path).exists():
                        return downloaded_path
                    else:
                        return None
                except FloodWaitError as e:
                    if attempt < 2:
                        await asyncio.sleep(e.seconds)
                    else:
                        return None
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        return None
            
            return None
        except Exception:
            return None

    async def update_media_path(self, channel: str, message_id: int, media_path: str):
        conn = self.get_db_connection(channel)
        conn.execute('UPDATE messages SET media_path = ? WHERE message_id = ?', 
                    (media_path, message_id))
        conn.commit()

    def should_forward_message(self, message, rule: ForwardingRule) -> bool:
        if not rule.enabled:
            return False
        
        if not message.media:
            return rule.forward_text and bool(message.message)
        
        if isinstance(message.media, MessageMediaWebPage):
            return rule.forward_text and bool(message.message)
        
        if isinstance(message.media, MessageMediaPhoto):
            return rule.forward_images
        
        if isinstance(message.media, MessageMediaDocument):
            if message.file:
                mime_type = getattr(message.file, 'mime_type', '') or ''
                if mime_type.startswith('video/'):
                    return rule.forward_videos
                else:
                    return rule.forward_documents
            return rule.forward_documents
        
        return False

    async def forward_message(self, message, rule: ForwardingRule, source_channel_id: int = None):
        try:
            if rule.destination_channel.lstrip('-').isdigit():
                dest_entity = await self.client.get_entity(PeerChannel(int(rule.destination_channel)))
            else:
                dest_entity = await self.client.get_entity(rule.destination_channel)
            
            if rule.forward_mode == "forward":
                await self.client.forward_messages(dest_entity, message)
                print(f"  ↪️ Forwarded message {message.id}")
            else:
                source_name = self.state.get('channel_names', {}).get(rule.source_channel, '')
                if source_channel_id:
                    full_id = f"-100{abs(source_channel_id)}" if not str(source_channel_id).startswith('-100') else str(source_channel_id)
                else:
                    full_id = rule.source_channel
                
                if source_name and source_name != 'no_username':
                    source_line = f"From: @{source_name} ({full_id})\n─────────────────\n"
                else:
                    source_line = f"From: {full_id}\n─────────────────\n"
                
                copy_text = source_line + (message.message or '')
                
                if message.media and not isinstance(message.media, MessageMediaWebPage):
                    await self.client.send_message(
                        dest_entity,
                        copy_text,
                        file=message.media
                    )
                else:
                    await self.client.send_message(dest_entity, copy_text)
                print(f"  📋 Copied message {message.id}")
                
            return True
        except FloodWaitError as e:
            print(f"  ⏳ Rate limited, waiting {e.seconds}s...")
            await asyncio.sleep(e.seconds)
            return await self.forward_message(message, rule, source_channel_id)
        except Exception as e:
            print(f"  ❌ Failed to forward message {message.id}: {e}")
            return False

    async def setup_forwarding_handler(self):
        rules = self.get_forwarding_rules()
        if not rules:
            print("No forwarding rules configured")
            return False
        
        enabled_rules = [r for r in rules if r.enabled]
        if not enabled_rules:
            print("No enabled forwarding rules")
            return False
        
        source_channels = []
        source_id_map = {}
        for rule in enabled_rules:
            try:
                if rule.source_channel.lstrip('-').isdigit():
                    channel_id = int(rule.source_channel)
                else:
                    entity = await self.client.get_entity(rule.source_channel)
                    channel_id = entity.id
                source_channels.append(channel_id)
                source_id_map[channel_id] = rule.source_channel
            except Exception as e:
                print(f"Failed to get entity for {rule.source_channel}: {e}")
        
        if not source_channels:
            print("No valid source channels")
            return False
        
        @self.client.on(events.NewMessage(chats=source_channels, incoming=True, outgoing=True))
        async def forwarding_handler(event):
            message = event.message
            chat_id = event.chat_id
            
            for rule in enabled_rules:
                rule_source_id = None
                if rule.source_channel.lstrip('-').isdigit():
                    rule_source_id = int(rule.source_channel)
                else:
                    rule_source_id = next((k for k, v in source_id_map.items() if v == rule.source_channel), None)
                
                if rule_source_id == chat_id:
                    if self.should_forward_message(message, rule):
                        source_name = self.state.get('channel_names', {}).get(rule.source_channel, rule.source_channel)
                        dest_name = self.state.get('channel_names', {}).get(rule.destination_channel, rule.destination_channel)
                        print(f"\n📨 New message in {source_name} → forwarding to {dest_name}")
                        await self.forward_message(message, rule, chat_id)
        
        self.forwarding_handler = forwarding_handler
        return True

    async def start_forwarding(self):
        self.forwarding_active = True
        
        if not self.client.is_connected():
            await self.client.connect()
        
        if not await self.setup_forwarding_handler():
            self.forwarding_active = False
            return
        
        rules = self.get_forwarding_rules()
        enabled_rules = [r for r in rules if r.enabled]
        
        print(f"\n🚀 Forwarding service started!")
        print(f"   Monitoring {len(enabled_rules)} rule(s)")
        print("   Press Ctrl+C to stop\n")
        
        for rule in enabled_rules:
            source_name = self.state.get('channel_names', {}).get(rule.source_channel, rule.source_channel)
            dest_name = self.state.get('channel_names', {}).get(rule.destination_channel, rule.destination_channel)
            content_types = []
            if rule.forward_text: content_types.append("text")
            if rule.forward_images: content_types.append("images")
            if rule.forward_videos: content_types.append("videos")
            if rule.forward_documents: content_types.append("documents")
            print(f"   • {source_name} → {dest_name}")
            print(f"     Mode: {rule.forward_mode} | Content: {', '.join(content_types)}")
        
        print("\n👀 Watching for new messages...\n")
        
        try:
            await self.client.run_until_disconnected()
        except asyncio.CancelledError:
            pass
        except ConnectionError:
            print("\n⚠️ Connection lost. Returning to menu...")
        except Exception as e:
            print(f"\n⚠️ Forwarding stopped: {e}")
        finally:
            self.forwarding_active = False
            print("\n⏹️ Forwarding service stopped")

    async def manage_forwarding_rules(self):
        while True:
            print("\n" + "="*45)
            print("         FORWARDING RULES MANAGER")
            print("="*45)
            
            rules = self.get_forwarding_rules()
            if rules:
                print("\nCurrent Rules:")
                for i, rule in enumerate(rules, 1):
                    source_name = self.state.get('channel_names', {}).get(rule.source_channel, rule.source_channel)
                    dest_name = self.state.get('channel_names', {}).get(rule.destination_channel, rule.destination_channel)
                    status = "✅" if rule.enabled else "❌"
                    content_types = []
                    if rule.forward_text: content_types.append("T")
                    if rule.forward_images: content_types.append("I")
                    if rule.forward_videos: content_types.append("V")
                    if rule.forward_documents: content_types.append("D")
                    print(f"  [{i}] {status} {source_name} → {dest_name}")
                    print(f"      Mode: {rule.forward_mode} | Content: {'/'.join(content_types)}")
            else:
                print("\nNo forwarding rules configured")
            
            print("\n[A] Add new rule")
            print("[E] Edit rule")
            print("[T] Toggle rule on/off")
            print("[D] Delete rule")
            print("[S] Start forwarding")
            print("[B] Back to main menu")
            print("="*45)
            
            choice = input("Enter your choice: ").lower().strip()
            
            if choice == 'a':
                await self.add_forwarding_rule_interactive()
            elif choice == 'e':
                await self.edit_forwarding_rule_interactive()
            elif choice == 't':
                await self.toggle_forwarding_rule()
            elif choice == 'd':
                await self.delete_forwarding_rule_interactive()
            elif choice == 's':
                if not rules or not any(r.enabled for r in rules):
                    print("❌ No enabled forwarding rules. Add or enable rules first.")
                    continue
                try:
                    await self.start_forwarding()
                except KeyboardInterrupt:
                    self.forwarding_active = False
                    print("\nForwarding stopped")
            elif choice == 'b':
                break
            else:
                print("Invalid option")

    async def add_forwarding_rule_interactive(self):
        print("\n📝 ADD NEW FORWARDING RULE")
        print("-" * 40)
        
        await self.view_channels()
        
        if not self.state['channels']:
            print("\n❌ No channels available. Add channels first using [L] in the main menu.")
            return
        
        channels_list = list(self.state['channels'].keys())
        
        print("\n1️⃣ Select SOURCE channel (messages will be forwarded FROM here):")
        source_input = input("Enter channel number or ID: ").strip()
        source_channels = self.parse_channel_selection(source_input)
        
        if not source_channels:
            print("❌ Invalid source channel selection")
            return
        source_channel = source_channels[0]
        
        print("\n2️⃣ Select DESTINATION channel (messages will be forwarded TO here):")
        print("   You can enter a channel number from the list, or a channel username (@channelname)")
        dest_input = input("Enter channel number, ID, or @username: ").strip()
        
        if dest_input.startswith('@'):
            dest_channel = dest_input
        else:
            dest_channels = self.parse_channel_selection(dest_input)
            if not dest_channels:
                print("❌ Invalid destination channel selection")
                return
            dest_channel = dest_channels[0]
        
        print("\n3️⃣ Select content types to forward:")
        print("   [1] Text messages")
        print("   [2] Images/Photos")
        print("   [3] Videos")
        print("   [4] Documents/Files")
        print("   [A] All types")
        print("   Example: 1,2,3 or A for all")
        
        content_input = input("Enter selection: ").strip().lower()
        
        if content_input == 'a':
            forward_text = forward_images = forward_videos = forward_documents = True
        else:
            selections = [x.strip() for x in content_input.split(',')]
            forward_text = '1' in selections
            forward_images = '2' in selections
            forward_videos = '3' in selections
            forward_documents = '4' in selections
        
        if not any([forward_text, forward_images, forward_videos, forward_documents]):
            print("❌ You must select at least one content type")
            return
        
        print("\n4️⃣ Select forwarding mode:")
        print("   [1] Copy - Send as new message (no 'Forwarded from' header)")
        print("   [2] Forward - Keep 'Forwarded from' header")
        
        mode_input = input("Enter 1 or 2: ").strip()
        forward_mode = "forward" if mode_input == '2' else "copy"
        
        rule = ForwardingRule(
            source_channel=source_channel,
            destination_channel=dest_channel,
            forward_text=forward_text,
            forward_images=forward_images,
            forward_videos=forward_videos,
            forward_documents=forward_documents,
            forward_mode=forward_mode,
            enabled=True
        )
        
        self.save_forwarding_rule(rule)
        
        source_name = self.state.get('channel_names', {}).get(source_channel, source_channel)
        dest_name = dest_channel if dest_channel.startswith('@') else self.state.get('channel_names', {}).get(dest_channel, dest_channel)
        
        print(f"\n✅ Forwarding rule created!")
        print(f"   {source_name} → {dest_name}")
        content_types = []
        if forward_text: content_types.append("text")
        if forward_images: content_types.append("images")
        if forward_videos: content_types.append("videos")
        if forward_documents: content_types.append("documents")
        print(f"   Content: {', '.join(content_types)}")
        print(f"   Mode: {forward_mode}")

    async def edit_forwarding_rule_interactive(self):
        rules = self.get_forwarding_rules()
        if not rules:
            print("No rules to edit")
            return
        
        try:
            idx = int(input("Enter rule number to edit: ")) - 1
            if not (0 <= idx < len(rules)):
                print("Invalid rule number")
                return
        except ValueError:
            print("Invalid input")
            return
        
        rule = rules[idx]
        
        print(f"\nEditing rule {idx + 1}:")
        print("Press Enter to keep current value\n")
        
        print("Current content types:")
        print(f"  Text: {'Yes' if rule.forward_text else 'No'}")
        print(f"  Images: {'Yes' if rule.forward_images else 'No'}")
        print(f"  Videos: {'Yes' if rule.forward_videos else 'No'}")
        print(f"  Documents: {'Yes' if rule.forward_documents else 'No'}")
        
        update_content = input("\nUpdate content types? (y/n): ").lower().strip()
        if update_content == 'y':
            print("Enter 1,2,3,4 or A for all:")
            print("   [1] Text  [2] Images  [3] Videos  [4] Documents")
            content_input = input("Selection: ").strip().lower()
            
            if content_input == 'a':
                rule.forward_text = rule.forward_images = rule.forward_videos = rule.forward_documents = True
            elif content_input:
                selections = [x.strip() for x in content_input.split(',')]
                rule.forward_text = '1' in selections
                rule.forward_images = '2' in selections
                rule.forward_videos = '3' in selections
                rule.forward_documents = '4' in selections
        
        print(f"\nCurrent mode: {rule.forward_mode}")
        print("   [1] Copy  [2] Forward")
        mode_input = input("New mode (or Enter to keep): ").strip()
        if mode_input == '1':
            rule.forward_mode = 'copy'
        elif mode_input == '2':
            rule.forward_mode = 'forward'
        
        self.save_forwarding_rule(rule)
        print("✅ Rule updated!")

    async def toggle_forwarding_rule(self):
        rules = self.get_forwarding_rules()
        if not rules:
            print("No rules to toggle")
            return
        
        try:
            idx = int(input("Enter rule number to toggle: ")) - 1
            if not (0 <= idx < len(rules)):
                print("Invalid rule number")
                return
        except ValueError:
            print("Invalid input")
            return
        
        rule = rules[idx]
        rule.enabled = not rule.enabled
        self.save_forwarding_rule(rule)
        
        status = "enabled" if rule.enabled else "disabled"
        print(f"✅ Rule {idx + 1} {status}")

    async def delete_forwarding_rule_interactive(self):
        rules = self.get_forwarding_rules()
        if not rules:
            print("No rules to delete")
            return
        
        try:
            idx = int(input("Enter rule number to delete: ")) - 1
            if not (0 <= idx < len(rules)):
                print("Invalid rule number")
                return
        except ValueError:
            print("Invalid input")
            return
        
        confirm = input(f"Delete rule {idx + 1}? (y/n): ").lower().strip()
        if confirm == 'y':
            if self.remove_forwarding_rule(idx):
                print("✅ Rule deleted")
            else:
                print("❌ Failed to delete rule")

    async def scrape_channel(self, channel: str, offset_id: int):
        try:
            if not self.client.is_connected():
                await self.client.connect()
            
            entity = await self.client.get_entity(PeerChannel(int(channel)) if channel.startswith('-') else channel)
            result = await self.client.get_messages(entity, offset_id=offset_id, reverse=True, limit=0)
            total_messages = result.total

            if total_messages == 0:
                print(f"No messages found in channel {channel}")
                return

            print(f"Found {total_messages} messages in channel {channel}")

            message_batch = []
            media_tasks = []
            processed_messages = 0
            last_message_id = offset_id
            semaphore = asyncio.Semaphore(self.max_concurrent_downloads)

            async for message in self.client.iter_messages(entity, offset_id=offset_id, reverse=True):
                try:
                    sender = await message.get_sender()

                    reactions_str = None
                    if message.reactions and message.reactions.results:
                        reactions_parts = []
                        for reaction in message.reactions.results:
                            emoji = getattr(reaction.reaction, 'emoticon', '')
                            count = reaction.count
                            if emoji:
                                reactions_parts.append(f"{emoji} {count}")
                        if reactions_parts:
                            reactions_str = ' '.join(reactions_parts)

                    msg_data = MessageData(
                        message_id=message.id,
                        date=message.date.strftime('%Y-%m-%d %H:%M:%S'),
                        sender_id=message.sender_id,
                        first_name=getattr(sender, 'first_name', None) if isinstance(sender, User) else None,
                        last_name=getattr(sender, 'last_name', None) if isinstance(sender, User) else None,
                        username=getattr(sender, 'username', None) if isinstance(sender, User) else None,
                        message=message.message or '',
                        media_type=message.media.__class__.__name__ if message.media else None,
                        media_path=None,
                        reply_to=message.reply_to_msg_id if message.reply_to else None,
                        post_author=message.post_author,
                        views=message.views,
                        forwards=message.forwards,
                        reactions=reactions_str
                    )

                    message_batch.append(msg_data)

                    if self.state['scrape_media'] and message.media and not isinstance(message.media, MessageMediaWebPage):
                        media_tasks.append(message)

                    last_message_id = message.id
                    processed_messages += 1

                    if len(message_batch) >= self.batch_size:
                        self.batch_insert_messages(channel, message_batch)
                        message_batch.clear()

                    if processed_messages % self.state_save_interval == 0:
                        self.state['channels'][channel] = last_message_id
                        self.save_state()

                    progress = (processed_messages / total_messages) * 100
                    bar_length = 30
                    filled_length = int(bar_length * processed_messages // total_messages)
                    bar = '█' * filled_length + '░' * (bar_length - filled_length)
                    
                    sys.stdout.write(f"\r📄 Messages: [{bar}] {progress:.1f}% ({processed_messages}/{total_messages})")
                    sys.stdout.flush()

                except Exception as e:
                    print(f"\nError processing message {message.id}: {e}")

            if message_batch:
                self.batch_insert_messages(channel, message_batch)

            if media_tasks:
                total_media = len(media_tasks)
                completed_media = 0
                successful_downloads = 0
                print(f"\n📥 Downloading {total_media} media files...")
                
                semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
                
                async def download_single_media(message):
                    async with semaphore:
                        return await self.download_media(channel, message)
                
                batch_size = 10
                for i in range(0, len(media_tasks), batch_size):
                    batch = media_tasks[i:i + batch_size]
                    tasks = [asyncio.create_task(download_single_media(msg)) for msg in batch]
                    
                    for j, task in enumerate(tasks):
                        try:
                            media_path = await task
                            if media_path:
                                await self.update_media_path(channel, batch[j].id, media_path)
                                successful_downloads += 1
                        except Exception:
                            pass
                        
                        completed_media += 1
                        progress = (completed_media / total_media) * 100
                        bar_length = 30
                        filled_length = int(bar_length * completed_media // total_media)
                        bar = '█' * filled_length + '░' * (bar_length - filled_length)
                        
                        sys.stdout.write(f"\r📥 Media: [{bar}] {progress:.1f}% ({completed_media}/{total_media})")
                        sys.stdout.flush()
                
                print(f"\n✅ Media download complete! ({successful_downloads}/{total_media} successful)")

            self.state['channels'][channel] = last_message_id
            self.save_state()
            print(f"\nCompleted scraping channel {channel}")

        except Exception as e:
            print(f"Error with channel {channel}: {e}")

    async def rescrape_media(self, channel: str):
        conn = self.get_db_connection(channel)
        cursor = conn.cursor()
        cursor.execute('SELECT message_id FROM messages WHERE media_type IS NOT NULL AND media_type != "MessageMediaWebPage" AND media_path IS NULL')
        message_ids = [row[0] for row in cursor.fetchall()]

        channel_name = self.state.get('channel_names', {}).get(channel, 'Unknown')

        if not message_ids:
            print(f"No media files to reprocess for {channel_name} (ID: {channel})")
            return

        print(f"📥 Reprocessing {len(message_ids)} media files for {channel_name} (ID: {channel})")

        try:
            if channel.lstrip('-').isdigit():
                entity = await self.client.get_entity(PeerChannel(int(channel)))
            else:
                entity = await self.client.get_entity(channel)
            semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
            completed_media = 0
            successful_downloads = 0
            
            async def download_single_media(message):
                async with semaphore:
                    return await self.download_media(channel, message)

            batch_size = 10
            for i in range(0, len(message_ids), batch_size):
                batch_ids = message_ids[i:i + batch_size]
                messages = await self.client.get_messages(entity, ids=batch_ids)
                
                valid_messages = [msg for msg in messages if msg and msg.media and not isinstance(msg.media, MessageMediaWebPage)]
                tasks = [asyncio.create_task(download_single_media(msg)) for msg in valid_messages]

                for j, task in enumerate(tasks):
                    try:
                        media_path = await task
                        if media_path:
                            await self.update_media_path(channel, valid_messages[j].id, media_path)
                            successful_downloads += 1
                    except Exception:
                        pass
                    
                    completed_media += 1
                    progress = (completed_media / len(message_ids)) * 100
                    bar_length = 30
                    filled_length = int(bar_length * completed_media // len(message_ids))
                    bar = '█' * filled_length + '░' * (bar_length - filled_length)
                    
                    sys.stdout.write(f"\r🔄 Rescrape: [{bar}] {progress:.1f}% ({completed_media}/{len(message_ids)})")
                    sys.stdout.flush()

            print(f"\n✅ Media reprocessing complete! ({successful_downloads}/{len(message_ids)} successful)")

        except Exception as e:
            print(f"Error reprocessing media: {e}")

    async def fix_missing_media(self, channel: str):
        conn = self.get_db_connection(channel)
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) FROM messages WHERE media_type IS NOT NULL AND media_type != "MessageMediaWebPage"')
        total_with_media = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM messages WHERE media_type IS NOT NULL AND media_type != "MessageMediaWebPage" AND media_path IS NOT NULL')
        total_with_files = cursor.fetchone()[0]

        missing_count = total_with_media - total_with_files

        channel_name = self.state.get('channel_names', {}).get(channel, 'Unknown')
        print(f"\n📊 Media Analysis for {channel_name} (ID: {channel}):")
        print(f"Messages with media: {total_with_media}")
        print(f"Media files downloaded: {total_with_files}")
        print(f"Missing media files: {missing_count}")

        if missing_count == 0:
            print("✅ All media files are already downloaded!")
            return

        cursor.execute('SELECT message_id, media_type FROM messages WHERE media_type IS NOT NULL AND media_type != "MessageMediaWebPage" AND (media_path IS NULL OR media_path = "")')
        missing_media = cursor.fetchall()

        if not missing_media:
            print("✅ No missing media found!")
            return

        print(f"\n🔧 Attempting to download {len(missing_media)} missing media files...")

        try:
            if channel.lstrip('-').isdigit():
                entity = await self.client.get_entity(PeerChannel(int(channel)))
            else:
                entity = await self.client.get_entity(channel)
            semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
            completed_media = 0
            successful_downloads = 0
            
            async def download_single_media(message):
                async with semaphore:
                    return await self.download_media(channel, message)
            
            batch_size = 10
            for i in range(0, len(missing_media), batch_size):
                batch = missing_media[i:i + batch_size]
                message_ids = [msg[0] for msg in batch]
                
                messages = await self.client.get_messages(entity, ids=message_ids)
                valid_messages = [msg for msg in messages if msg and msg.media and not isinstance(msg.media, MessageMediaWebPage)]
                
                tasks = [asyncio.create_task(download_single_media(msg)) for msg in valid_messages]

                for j, task in enumerate(tasks):
                    try:
                        media_path = await task
                        if media_path:
                            await self.update_media_path(channel, valid_messages[j].id, media_path)
                            successful_downloads += 1
                    except Exception:
                        pass
                    
                    completed_media += 1
                    progress = (completed_media / len(missing_media)) * 100
                    bar_length = 30
                    filled_length = int(bar_length * completed_media // len(missing_media))
                    bar = '█' * filled_length + '░' * (bar_length - filled_length)
                    
                    sys.stdout.write(f"\r🔧 Fix Media: [{bar}] {progress:.1f}% ({completed_media}/{len(missing_media)})")
                    sys.stdout.flush()

            print(f"\n✅ Media fix complete! ({successful_downloads}/{len(missing_media)} successful)")

        except Exception as e:
            print(f"Error fixing missing media: {e}")

    async def continuous_scraping(self):
        self.continuous_scraping_active = True
        
        try:
            while self.continuous_scraping_active:
                start_time = time.time()
                
                for channel in self.state['channels']:
                    if not self.continuous_scraping_active:
                        break
                    print(f"\nChecking for new messages in channel: {channel}")
                    await self.scrape_channel(channel, self.state['channels'][channel])
                
                elapsed = time.time() - start_time
                sleep_time = max(0, 60 - elapsed)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    
        except asyncio.CancelledError:
            print("Continuous scraping stopped")
        finally:
            self.continuous_scraping_active = False

    def get_export_filename(self, channel: str):
        username = self.state.get('channel_names', {}).get(channel, 'no_username')
        return f"{channel}_{username}"

    def export_to_csv(self, channel: str):
        conn = self.get_db_connection(channel)
        filename = self.get_export_filename(channel)
        csv_file = Path(channel) / f'{filename}.csv'

        cursor = conn.cursor()
        cursor.execute('SELECT * FROM messages ORDER BY date')
        columns = [description[0] for description in cursor.description]

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)

            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                writer.writerows(rows)

    def export_to_json(self, channel: str):
        conn = self.get_db_connection(channel)
        filename = self.get_export_filename(channel)
        json_file = Path(channel) / f'{filename}.json'

        cursor = conn.cursor()
        cursor.execute('SELECT * FROM messages ORDER BY date')
        columns = [description[0] for description in cursor.description]

        with open(json_file, 'w', encoding='utf-8') as f:
            f.write('[\n')
            first_row = True

            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break

                for row in rows:
                    if not first_row:
                        f.write(',\n')
                    else:
                        first_row = False

                    data = dict(zip(columns, row))
                    json.dump(data, f, ensure_ascii=False, indent=2)

            f.write('\n]')

    async def export_data(self):
        if not self.state['channels']:
            print("No channels to export")
            return
            
        for channel in self.state['channels']:
            print(f"Exporting data for channel {channel}...")
            try:
                self.export_to_csv(channel)
                self.export_to_json(channel)
                print(f"✅ Completed export for channel {channel}")
            except Exception as e:
                print(f"❌ Export failed for channel {channel}: {e}")

    async def view_channels(self):
        if not self.state['channels']:
            print("No channels saved")
            return

        if not self.client.is_connected():
            await self.client.connect()

        print("\nCurrent channels:")
        for i, (channel, last_id) in enumerate(self.state['channels'].items(), 1):
            try:
                channel_name = self.state.get('channel_names', {}).get(channel)
                
                if not channel_name or channel_name in ['Unknown', 'no_username']:
                    try:
                        if channel.lstrip('-').isdigit():
                            entity = await self.client.get_entity(PeerChannel(int(channel)))
                        else:
                            entity = await self.client.get_entity(channel)
                        channel_name = getattr(entity, 'title', None) or getattr(entity, 'username', None) or 'Unknown'
                        if 'channel_names' not in self.state:
                            self.state['channel_names'] = {}
                        self.state['channel_names'][channel] = channel_name
                        self.save_state()
                    except:
                        channel_name = 'Unknown'
                
                conn = self.get_db_connection(channel)
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM messages')
                count = cursor.fetchone()[0]
                print(f"[{i}] {channel_name} (ID: {channel}), Last Message ID: {last_id}, Messages: {count}")
            except:
                channel_name = self.state.get('channel_names', {}).get(channel, 'Unknown')
                print(f"[{i}] {channel_name} (ID: {channel}), Last Message ID: {last_id}")

    async def list_channels(self):
        try:
            if not self.client.is_connected():
                await self.client.connect()
            
            print("\nList of channels and groups joined by account:")
            count = 1
            channels_data = []
            async for dialog in self.client.iter_dialogs():
                entity = dialog.entity
                if dialog.id != 777000 and (isinstance(entity, Channel) or isinstance(entity, Chat)):
                    channel_type = "Channel" if isinstance(entity, Channel) and entity.broadcast else "Group"
                    username = getattr(entity, 'username', None) or 'no_username'
                    print(f"[{count}] {dialog.title} (ID: {dialog.id}, Type: {channel_type}, Username: @{username})")
                    channels_data.append({
                        'number': count,
                        'channel_name': dialog.title,
                        'channel_id': str(dialog.id),
                        'username': username,
                        'type': channel_type
                    })
                    count += 1

            if channels_data:
                csv_file = Path('channels_list.csv')
                with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=['number', 'channel_name', 'channel_id', 'username', 'type'])
                    writer.writeheader()
                    writer.writerows(channels_data)
                print(f"\n✅ Saved channels list to {csv_file}")

            return channels_data

        except Exception as e:
            print(f"Error listing channels: {e}")
            return []

    def display_qr_code_ascii(self, qr_login):
        qr = qrcode.QRCode(box_size=1, border=1)
        qr.add_data(qr_login.url)
        qr.make()
        
        f = StringIO()
        qr.print_ascii(out=f)
        f.seek(0)
        print(f.read())

    async def qr_code_auth(self):
        print("\nChoosing QR Code authentication...")
        print("Please scan the QR code with your Telegram app:")
        print("1. Open Telegram on your phone")
        print("2. Go to Settings > Devices > Scan QR")
        print("3. Scan the code below\n")
        
        qr_login = await self.client.qr_login()
        self.display_qr_code_ascii(qr_login)
        
        try:
            await qr_login.wait()
            print("\n✅ Successfully logged in via QR code!")
            return True
        except SessionPasswordNeededError:
            password = input("Two-factor authentication enabled. Enter your password: ")
            await self.client.sign_in(password=password)
            print("\n✅ Successfully logged in with 2FA!")
            return True
        except Exception as e:
            print(f"\n❌ QR code authentication failed: {e}")
            return False

    async def phone_auth(self):
        phone = input("Enter your phone number: ")
        await self.client.send_code_request(phone)
        code = input("Enter the code you received: ")
        
        try:
            await self.client.sign_in(phone, code)
            print("\n✅ Successfully logged in via phone!")
            return True
        except SessionPasswordNeededError:
            password = input("Two-factor authentication enabled. Enter your password: ")
            await self.client.sign_in(password=password)
            print("\n✅ Successfully logged in with 2FA!")
            return True
        except Exception as e:
            print(f"\n❌ Phone authentication failed: {e}")
            return False

    async def initialize_client(self):
        if not all([self.state.get('api_id'), self.state.get('api_hash')]):
            print("\n=== API Configuration Required ===")
            print("You need to provide API credentials from https://my.telegram.org")
            try:
                self.state['api_id'] = int(input("Enter your API ID: "))
                self.state['api_hash'] = input("Enter your API Hash: ")
                self.save_state()
            except ValueError:
                print("Invalid API ID. Must be a number.")
                return False

        self.client = TelegramClient('session', self.state['api_id'], self.state['api_hash'])
        
        try:
            await self.client.connect()
        except Exception as e:
            print(f"Failed to connect: {e}")
            return False
        
        if not await self.client.is_user_authorized():
            print("\n=== Choose Authentication Method ===")
            print("[1] QR Code (Recommended - No phone number needed)")
            print("[2] Phone Number (Traditional method)")
            
            while True:
                choice = input("Enter your choice (1 or 2): ").strip()
                if choice in ['1', '2']:
                    break
                print("Please enter 1 or 2")
            
            success = await self.qr_code_auth() if choice == '1' else await self.phone_auth()
                
            if not success:
                print("Authentication failed. Please try again.")
                await self.client.disconnect()
                return False
        else:
            print("✅ Already authenticated!")
            
        return True

    def parse_channel_selection(self, choice):
        channels_list = list(self.state['channels'].keys())
        selected_channels = []
        
        if choice.lower() == 'all':
            return channels_list
        
        for selection in [x.strip() for x in choice.split(',')]:
            try:
                if selection.startswith('-'):
                    if selection in self.state['channels']:
                        selected_channels.append(selection)
                    else:
                        print(f"Channel ID {selection} not found in your channels")
                else:
                    num = int(selection)
                    if 1 <= num <= len(channels_list):
                        selected_channels.append(channels_list[num - 1])
                    else:
                        print(f"Invalid channel number: {num}. Valid range: 1-{len(channels_list)}")
            except ValueError:
                print(f"Invalid input: {selection}. Use numbers (1,2,3) or full IDs (-100123...)")
        
        return selected_channels

    async def scrape_specific_channels(self):
        if not self.state['channels']:
            print("No channels available. Use [L] to add channels first")
            return

        await self.view_channels()
        print("\n📥 Scrape Options:")
        print("• Single: 1 or -1001234567890")
        print("• Multiple: 1,3,5 or mix formats")
        print("• All channels: all")
        
        choice = input("\nEnter selection: ").strip()
        selected_channels = self.parse_channel_selection(choice)
        
        if selected_channels:
            print(f"\n🚀 Starting scrape of {len(selected_channels)} channel(s)...")
            for i, channel in enumerate(selected_channels, 1):
                print(f"\n[{i}/{len(selected_channels)}] Scraping: {channel}")
                await self.scrape_channel(channel, self.state['channels'][channel])
            print(f"\n✅ Completed scraping {len(selected_channels)} channel(s)!")
        else:
            print("❌ No valid channels selected")

    async def manage_channels(self):
        while True:
            print("\n" + "="*40)
            print("           TELEGRAM SCRAPER")
            print("="*40)
            print("[S] Scrape channels")
            print("[C] Continuous scraping")
            print(f"[M] Media scraping: {'ON' if self.state['scrape_media'] else 'OFF'}")
            print("[L] List & add channels")
            print("[R] Remove channels")
            print("[E] Export data")
            print("[T] Rescrape media")
            print("[F] Fix missing media")
            print("[W] Forwarding rules")
            print("[Q] Quit")
            print("="*40)

            choice = input("Enter your choice: ").lower().strip()
            
            try:
                if choice == 'r':
                    if not self.state['channels']:
                        print("No channels to remove")
                        continue
                        
                    await self.view_channels()
                    print("\nTo remove channels:")
                    print("• Single: 1 or -1001234567890")
                    print("• Multiple: 1,2,3 or mix formats")
                    selection = input("Enter selection: ").strip()
                    selected_channels = self.parse_channel_selection(selection)
                    
                    if selected_channels:
                        removed_count = 0
                        for channel in selected_channels:
                            if channel in self.state['channels']:
                                del self.state['channels'][channel]
                                print(f"✅ Removed channel {channel}")
                                removed_count += 1
                            else:
                                print(f"❌ Channel {channel} not found")
                        
                        if removed_count > 0:
                            self.save_state()
                            print(f"\n🎉 Removed {removed_count} channel(s)!")
                            await self.view_channels()
                        else:
                            print("No channels were removed")
                    else:
                        print("No valid channels selected")
                        
                elif choice == 's':
                    await self.scrape_specific_channels()
                    
                elif choice == 'm':
                    self.state['scrape_media'] = not self.state['scrape_media']
                    self.save_state()
                    print(f"\n✅ Media scraping {'enabled' if self.state['scrape_media'] else 'disabled'}")
                    
                elif choice == 'c':
                    task = asyncio.create_task(self.continuous_scraping())
                    print("Continuous scraping started. Press Ctrl+C to stop.")
                    try:
                        await asyncio.sleep(float('inf'))
                    except KeyboardInterrupt:
                        self.continuous_scraping_active = False
                        task.cancel()
                        print("\nStopping continuous scraping...")
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                            
                elif choice == 'e':
                    await self.export_data()
                    
                elif choice == 'l':
                    channels_data = await self.list_channels()

                    if not channels_data:
                        continue

                    print("\nTo add channels from the list above:")
                    print("• Single: 1 or -1001234567890")
                    print("• Multiple: 1,3,5 or mix formats")
                    print("• All channels: all")
                    print("• Press Enter to skip adding")
                    selection = input("\nEnter selection (or Enter to skip): ").strip()

                    if selection:
                        added_count = 0

                        if selection.lower() == 'all':
                            for channel_info in channels_data:
                                channel_id = channel_info['channel_id']
                                if channel_id not in self.state['channels']:
                                    self.state['channels'][channel_id] = 0
                                    if 'channel_names' not in self.state:
                                        self.state['channel_names'] = {}
                                    self.state['channel_names'][channel_id] = channel_info['username']
                                    print(f"✅ Added channel {channel_info['channel_name']} (ID: {channel_id})")
                                    added_count += 1
                                else:
                                    print(f"Channel {channel_info['channel_name']} already added")
                        else:
                            for sel in [x.strip() for x in selection.split(',')]:
                                try:
                                    if sel.startswith('-'):
                                        channel_id = sel
                                        channel_info = next((c for c in channels_data if c['channel_id'] == channel_id), None)
                                        if not channel_info:
                                            print(f"Channel ID {channel_id} not found")
                                            continue
                                    else:
                                        num = int(sel)
                                        if 1 <= num <= len(channels_data):
                                            channel_info = channels_data[num - 1]
                                            channel_id = channel_info['channel_id']
                                        else:
                                            print(f"Invalid number: {num}. Choose 1-{len(channels_data)}")
                                            continue

                                    if channel_id in self.state['channels']:
                                        print(f"Channel {channel_info['channel_name']} already added")
                                    else:
                                        self.state['channels'][channel_id] = 0
                                        if 'channel_names' not in self.state:
                                            self.state['channel_names'] = {}
                                        self.state['channel_names'][channel_id] = channel_info['username']
                                        print(f"✅ Added channel {channel_info['channel_name']} (ID: {channel_id})")
                                        added_count += 1

                                except ValueError:
                                    print(f"Invalid input: {sel}")

                        if added_count > 0:
                            self.save_state()
                            print(f"\n🎉 Added {added_count} new channel(s)!")
                            await self.view_channels()
                        else:
                            print("No new channels were added")
                    
                elif choice == 't':
                    if not self.state['channels']:
                        print("No channels available. Add channels first")
                        continue
                        
                    await self.view_channels()
                    print("\nEnter channel NUMBER (1,2,3...) or full channel ID (-100123...)")
                    selection = input("Enter your selection: ").strip()
                    selected_channels = self.parse_channel_selection(selection)
                    
                    if len(selected_channels) == 1:
                        channel = selected_channels[0]
                        print(f"Rescaping media for channel: {channel}")
                        await self.rescrape_media(channel)
                    elif len(selected_channels) > 1:
                        print("Please select only one channel for media rescaping")
                    else:
                        print("No valid channel selected")
                    
                elif choice == 'f':
                    if not self.state['channels']:
                        print("No channels available. Add channels first")
                        continue
                        
                    await self.view_channels()
                    print("\nEnter channel NUMBER (1,2,3...) or full channel ID (-100123...)")
                    selection = input("Enter your selection: ").strip()
                    selected_channels = self.parse_channel_selection(selection)
                    
                    if len(selected_channels) == 1:
                        channel = selected_channels[0]
                        await self.fix_missing_media(channel)
                    elif len(selected_channels) > 1:
                        print("Please select only one channel for fixing missing media")
                    else:
                        print("No valid channel selected")
                
                elif choice == 'w':
                    await self.manage_forwarding_rules()
                    
                elif choice == 'q':
                    print("\n👋 Goodbye!")
                    self.close_db_connections()
                    if self.client:
                        await self.client.disconnect()
                    sys.exit()
                    
                else:
                    print("Invalid option")
                    
            except Exception as e:
                print(f"Error: {e}")

    async def run(self):
        display_ascii_art()
        if await self.initialize_client():
            try:
                await self.manage_channels()
            finally:
                self.close_db_connections()
                if self.client:
                    await self.client.disconnect()
        else:
            print("Failed to initialize client. Exiting.")

async def main():
    scraper = OptimizedTelegramScraper()
    await scraper.run()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted. Exiting...")
        sys.exit()
