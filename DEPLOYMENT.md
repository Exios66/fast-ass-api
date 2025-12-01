# Deployment Guide

This guide covers deploying Fast-Ass API to various environments.

## Prerequisites

- Python 3.8+
- pip
- Docker (optional, for containerized deployment)
- A server with sufficient resources

## Environment Variables

Configure these environment variables:

```bash
# Data directory (default: ./data/csv)
CSV_DATA_DIR=/path/to/data/csv

# CORS origins (comma-separated, default: *)
CORS_ORIGINS=https://app.example.com,https://admin.example.com

# Maximum upload size in bytes (default: 10MB)
MAX_UPLOAD_SIZE=10485760

# Log level (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL=INFO
```

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run server
uvicorn api.server.main:app --reload --port 8000

# Access API
# http://localhost:8000/docs
```

## Docker Deployment

### Using Dockerfile

```bash
# Build image
docker build -t fast-ass-api:latest .

# Run container
docker run -d \
  -p 8000:8000 \
  -v /path/to/data:/app/data \
  -e CSV_DATA_DIR=/app/data/csv \
  -e CORS_ORIGINS=https://app.example.com \
  --name fast-ass-api \
  fast-ass-api:latest
```

### Using Docker Compose

```bash
# Start services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

## Production Deployment

### Systemd Service

Create `/etc/systemd/system/fast-ass-api.service`:

```ini
[Unit]
Description=Fast-Ass API
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory=/opt/fast-ass-api
Environment="PATH=/opt/fast-ass-api/.venv/bin"
Environment="CSV_DATA_DIR=/opt/fast-ass-api/data/csv"
Environment="CORS_ORIGINS=https://app.example.com"
ExecStart=/opt/fast-ass-api/.venv/bin/uvicorn api.server.main:app --host 0.0.0.0 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl enable fast-ass-api
sudo systemctl start fast-ass-api
sudo systemctl status fast-ass-api
```

### Nginx Reverse Proxy

Example Nginx configuration:

```nginx
server {
    listen 80;
    server_name api.example.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Increase timeouts for large uploads
        proxy_read_timeout 300s;
        proxy_connect_timeout 300s;
    }
}
```

### Gunicorn (Production WSGI Server)

```bash
# Install gunicorn
pip install gunicorn

# Run with gunicorn
gunicorn api.server.main:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --access-logfile - \
  --error-logfile -
```

## Cloud Deployments

### AWS (EC2/ECS)

1. Launch EC2 instance or ECS cluster
2. Install Python and dependencies
3. Configure security groups (port 8000)
4. Set environment variables
5. Run with systemd or ECS task definition

### Google Cloud Platform

```bash
# Deploy to Cloud Run
gcloud run deploy fast-ass-api \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars CSV_DATA_DIR=/app/data/csv
```

### Heroku

```bash
# Create Procfile
echo "web: uvicorn api.server.main:app --host 0.0.0.0 --port \$PORT" > Procfile

# Deploy
git push heroku main
```

### Railway

```bash
# Install Railway CLI
npm i -g @railway/cli

# Login and deploy
railway login
railway init
railway up
```

## Monitoring

### Health Checks

Monitor these endpoints:

- `/health` - Basic health check
- `/health/ready` - Readiness check
- `/metrics` - Application metrics

### Logging

Logs are output to stdout/stderr. For production:

```bash
# Redirect to file
uvicorn api.server.main:app > /var/log/fast-ass-api.log 2>&1

# Or use systemd journal
journalctl -u fast-ass-api -f
```

### Metrics

Access metrics at `/metrics` endpoint. Integrate with:
- Prometheus
- Grafana
- Datadog
- New Relic

## Backup

### Data Backup

```bash
# Backup CSV files
tar -czf backup-$(date +%Y%m%d).tar.gz /path/to/data/csv

# Backup metadata
tar -czf metadata-backup-$(date +%Y%m%d).tar.gz /path/to/data/metadata
```

### Automated Backups

Add to crontab:

```bash
# Daily backup at 2 AM
0 2 * * * tar -czf /backups/fast-ass-api-$(date +\%Y\%m\%d).tar.gz /opt/fast-ass-api/data
```

## Security

### Best Practices

1. **Use HTTPS**: Always use SSL/TLS in production
2. **Set CORS properly**: Don't use `*` in production
3. **Rate limiting**: Already built-in, but monitor usage
4. **Input validation**: All inputs are validated
5. **File permissions**: Restrict access to data directory
6. **Firewall**: Only expose necessary ports

### Security Headers

Add to Nginx:

```nginx
add_header X-Content-Type-Options nosniff;
add_header X-Frame-Options DENY;
add_header X-XSS-Protection "1; mode=block";
add_header Strict-Transport-Security "max-age=31536000; includeSubDomains";
```

## Scaling

### Horizontal Scaling

For multiple instances:
- Use shared storage (NFS, S3, etc.) for CSV files
- Or use a database instead of CSV files
- Use a load balancer (Nginx, HAProxy, AWS ALB)

### Vertical Scaling

- Increase server resources
- Optimize Python/uvicorn workers
- Use connection pooling if using a database

## Troubleshooting

### Common Issues

**Port already in use:**
```bash
# Find process
lsof -i :8000
# Kill process
kill -9 <PID>
```

**Permission denied:**
```bash
# Fix data directory permissions
chmod -R 755 /path/to/data
chown -R www-data:www-data /path/to/data
```

**CORS errors:**
- Check CORS_ORIGINS environment variable
- Verify origin is in allowed list

**High memory usage:**
- Reduce number of workers
- Implement pagination for large datasets
- Consider database migration for large datasets

## Rollback

```bash
# Stop service
sudo systemctl stop fast-ass-api

# Restore previous version
git checkout <previous-commit>

# Restart service
sudo systemctl start fast-ass-api
```

## Updates

```bash
# Pull latest changes
git pull origin main

# Install new dependencies
pip install -r requirements.txt

# Restart service
sudo systemctl restart fast-ass-api
```

