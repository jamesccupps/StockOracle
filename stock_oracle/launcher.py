"""
Stock Oracle Launcher
======================
Entry point for both development and frozen (PyInstaller) modes.
Handles path setup so data files go to the right place.
"""
import sys
import os


def get_app_dirs():
    """
    Returns (app_dir, data_dir) paths.
    
    In dev mode:
        app_dir = the repository root (parent of the stock_oracle package)
        data_dir = stock_oracle/data (inside the project)
    
    In frozen (PyInstaller) mode:
        app_dir = the directory containing the .exe
        data_dir = %APPDATA%/StockOracle (persistent across updates)
    """
    if getattr(sys, 'frozen', False):
        # Running as PyInstaller bundle
        app_dir = os.path.dirname(sys.executable)
        data_dir = os.path.join(
            os.environ.get("APPDATA", app_dir), "StockOracle"
        )
    else:
        # Running as normal Python. This file is inside the package, so the
        # package dir is its folder (data/ under it, as config.py defaults)
        # and the repo root, which must be on sys.path, is one up. Joining
        # "stock_oracle/data" onto the package dir went one level too deep.
        pkg_dir = os.path.dirname(os.path.abspath(__file__))
        app_dir = os.path.dirname(pkg_dir)
        data_dir = os.path.join(pkg_dir, "data")

    # Ensure data directories exist
    for subdir in ["", "predictions", "sessions", "models", "cache"]:
        path = os.path.join(data_dir, subdir)
        os.makedirs(path, exist_ok=True)

    return app_dir, data_dir


def main():
    # Set up environment before importing stock_oracle
    app_dir, data_dir = get_app_dirs()

    # Tell config.py where data lives (it checks this env var)
    os.environ["STOCK_ORACLE_DATA_DIR"] = data_dir

    # Add the app directory to path if needed
    if app_dir not in sys.path:
        sys.path.insert(0, app_dir)

    # Launch the GUI
    from stock_oracle.gui import main as gui_main
    gui_main()


if __name__ == "__main__":
    main()
