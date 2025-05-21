#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
from datetime import datetime
import os
import sys
from django.conf import settings
import webbrowser

def main():
    """Run administrative tasks."""
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "billingv3.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    
    from django.core.management.commands.runserver import Command as RunCommand

    def on_bind_patch_to_open_browser(self, server_port):
            quit_command = "CTRL-BREAK" if sys.platform == "win32" else "CONTROL-C"

            if self._raw_ipv6:
                addr = f"[{self.addr}]"
            elif self.addr == "0":
                addr = "0.0.0.0"
            else:
                addr = self.addr
                
            url = f"{self.protocol}://127.0.0.1:{server_port}/app/orders/"
            webbrowser.open(url)
            now = datetime.now().strftime("%B %d, %Y - %X")
            version = self.get_version()
            print(
                f"{now}\n"
                f"Django version {version}, using settings {settings.SETTINGS_MODULE!r}\n"
                f"Starting development server at {self.protocol}://{addr}:{server_port}/\n"
                f"Quit the server with {quit_command}.",
                file=self.stdout,
            )

    if sys.platform == "win32" : 
        RunCommand.on_bind = on_bind_patch_to_open_browser
    main()
