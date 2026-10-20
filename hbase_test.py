import happybase

HOST = "localhost"
PORT = 9090

print("Connecting to HBase...")

connection = happybase.Connection(
    host=HOST,
    port=PORT,
    timeout=10000
)

connection.open()

print("Connected to HBase!")

tables = connection.tables()

print("Available tables:")
for table in tables:
    print(" -", table.decode())

connection.close()

print("Connection closed.")
