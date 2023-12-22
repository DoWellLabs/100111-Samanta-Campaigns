from django.core.management.base import BaseCommand

from ..utils import (
    app_exists, get_apps_with_dbobjects,
    migrate_dbobjects_in_app,
    migrate_dbobjects_in_apps
)



class Command(BaseCommand):
    """
    Run using:
      `python manage.py migrate`
    """
    help = "Migrates DBObjects defined in `dbobjects.py` in a given app to the database"

    def add_arguments(self, parser):
        parser.add_argument(
            "--app", 
            type=str, 
            dest="app",
            help="Label of app to migrate"
        )
        parser.add_argument(
            "--all",
            action="store_true",
            dest="all",
            help="Migrate all apps with DBObjects", 
        )
        parser.add_argument(
            "--noinput",
            "--no-input",
            action="store_false",
            dest="interactive",
            help="Do NOT prompt the user for input of any kind.",
        )


    def handle(self, *args, **options):
        app_label = options.get("app", None)
        migrate_all_apps = options.get("all", False)
        # By default, the user will be prompted for input
        wants_interactive = options.get("interactive", True)

        if not any([migrate_all_apps, app_label]):
            self.stdout.write(
                "Migration operation cancelled.\n"
                "Specify an app or use the --all option"
            )
            return

        if migrate_all_apps and app_label:
            self.stdout.write(
                "You cannot specify an app and use the --all option at the same time"
            )
            return
        
        if app_label and not app_exists(app_label):
            self.stdout.write(
                f"App '{app_label}' is not installed. "
                f"Make sure it is added to settings.INSTALLED_APPS and try again."
            )
            return
        
        apps_with_dbobjects = get_apps_with_dbobjects()
        if not apps_with_dbobjects:
            self.stdout.write(
                "No apps with DBObjects found. "
                "Make sure you have a 'dbobjects.py' file in an app and try again."
            )
            return
        
        apps_with_dbobjects_labels = [ app.label for app in apps_with_dbobjects ]

        if app_label and app_label not in apps_with_dbobjects_labels:
            self.stdout.write(
                f"App '{app_label}' does not have a 'dbobjects.py' file. "
                f"Available apps with DBObjects are: {', '.join(apps_with_dbobjects_labels)}\n\n"
                f"Please specify an app with a 'dbobjects.py' file.\n\n"
                f"If you have DBObjects in an app that is not listed, "
                f"add a 'dbobjects.py' file to that app and move all DBObjects to that file."
            )
            return
        
        if wants_interactive:
            self.stdout.write(
                "You have requested to migrate DBObjects in {}.\n"
                "This will create tables or collections for DBObjects in the database.\n\n"
                "Are you sure you want to do this?\n\n"
                "Type 'yes' to continue, or 'no' to cancel: ".format(app_label or ', '.join(apps_with_dbobjects_labels))
            )
            if input().lower() != "yes":
                self.stdout.write("Migration operation cancelled")
                return
            self.stdout.write("\n")
            
        if migrate_all_apps:
            migrate_dbobjects_in_apps(apps_with_dbobjects_labels, output_stream=self.stdout)
        else:
            migrate_dbobjects_in_app(app_label, output_stream=self.stdout)
        return
