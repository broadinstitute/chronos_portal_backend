"""Concurrency utilities for thread-safe operations."""

import asyncio

# Global lock for matplotlib operations - ensures only one job generates figures at a time
# Acquired around each individual report generation call, not the entire sequence
matplotlib_lock = asyncio.Lock()
