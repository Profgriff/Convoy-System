FROM node:18

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
CMD ["sh", "-c", "npx wait-on tcp:redis:6379 && npm run dev"]
