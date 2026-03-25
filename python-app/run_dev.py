import uvicorn

if __name__ == '__main__':
    uvicorn.run(
        'app.main:app',
        host='0.0.0.0',
        port=8000,
        reload=True,
        ws_ping_interval=30,
        ws_ping_timeout=180,
        ws_max_size=16 * 1024 * 1024,
    )
