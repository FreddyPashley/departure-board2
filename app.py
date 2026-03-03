"""
Heathrow RFE 2026 - Desktop Application
Board: full-screen display for projector/livestream
Planner: interactive editor for gates/terminals
"""
import threading
import time
import webview
from server import app, init

def start_server():
    app.run(host="0.0.0.0", port=6767, debug=False, use_reloader=False)

if __name__ == "__main__":
    init()

    server = threading.Thread(target=start_server, daemon=True)
    server.start()
    time.sleep(1)

    planner_win = webview.create_window(
        "Heathrow RFE - Planner",
        "http://127.0.0.1:6767/planner",
        width=1400,
        height=900,
        resizable=True,
    )

    board_win = webview.create_window(
        "Heathrow RFE - Board",
        "http://127.0.0.1:6767/",
        fullscreen=True,
    )

    webview.start()
