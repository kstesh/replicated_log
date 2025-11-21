### 1. Відправка повідомлення на Master (POST /append)

curl -X POST http://localhost:8000/append -H "Content-Type: application/json" -d '{"message":"msg1","w":1}'

curl -X POST http://localhost:8000/append -H "Content-Type: application/json" -d '{"message":"msg2","w":2}'

curl -X POST http://localhost:8000/append -H "Content-Type: application/json" -d '{"message":"msg3","w":3}'

### 2. Перегляд повідомлень Master та Secondary (GET /messages)

**Master**

curl http://localhost:8000/messages

**Secondary 1**

curl http://localhost:8001/messages

**Secondary 2**

curl http://localhost:8002/messages
