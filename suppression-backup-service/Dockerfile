ARG GO_VERSION=1.20
ARG ALPINE_VERSION=3.17
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder

# Create a new directory for our app
RUN mkdir /app

# Set the working directory to /app
WORKDIR /app

# Copy mod file
COPY ../go.mod .

# Copy sum file
COPY ../go.sum .

# Download dependencies
RUN go mod download

# Copy the source from the current directory to the working directory inside the container
COPY . .

# Build the app
RUN go build -o main ./suppression-backup-service/cmd/main.go

# Set the command to run when the container starts
CMD ["./main"]
