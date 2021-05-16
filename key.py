from pykeepass import PyKeePass

kp = PyKeePass('c:/xxx/KeePassDB.kdbx',password='wh0CanGuessThis1')

group = kp.find_groups(name='PREMSPROMS')


group.entries


entry = kp.find_entries(title='ITF ftp user premsproms', first=True)

entry.password

entry1 = kp.find_entries(username='premspromsuser', first=True)

entry1.password

