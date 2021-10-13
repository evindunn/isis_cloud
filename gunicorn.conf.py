from os import cpu_count

bind = "0.0.0.0:8080"
workers = min(12, cpu_count() * 2 + 1)
loglevel = "info"
timeout = 24 * 3600
