"""
Stock Oracle — Main Entry Point
================================
Launches the GUI by default, or the CLI if --cli flag is passed.

    python -m stock_oracle          # Launch GUI
    python -m stock_oracle --cli    # CLI mode
    python -m stock_oracle.oracle   # Direct CLI (original)
    python -m stock_oracle.gui      # Direct GUI
"""
import sys

if __name__ == "__main__":
    if "--cli" in sys.argv or len(sys.argv) > 1 and sys.argv[1] not in ("--cli",):
        # CLI mode: pass through to oracle
        from stock_oracle.oracle import main
        main()
    else:
        # GUI mode (default)
        from stock_oracle.gui import main
        main()
