FROM node:20-alpine

WORKDIR /app

# Install dependencies first (cache)
COPY package*.json ./
RUN npm install

# Copy source
COPY . .

# Development mode (pas besoin de build)
EXPOSE 3000

CMD ["npm", "run", "dev"]