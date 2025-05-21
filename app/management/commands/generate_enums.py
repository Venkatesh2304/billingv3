# myapp/management/commands/generate_enum_js.py

import os
import importlib
import inspect
import enum
from django.core.management.base import BaseCommand
from django.db import models


class Command(BaseCommand):
    help = 'Generate JavaScript enum constants and options from Django or Python enums'

    def handle(self, *args, **kwargs):
        output_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'billing_refine', 'enums.js')
        )

        enums_module = importlib.import_module('app.enums')  # adjust to your path

        with open(output_path, 'w+') as js_file:
            js_file.write("// ⚠️ Auto-generated from Django + Python enums. Do not edit manually.\n\n")

            for name, obj in enums_module.__dict__.items():
                if not inspect.isclass(obj):
                    continue
                
                if issubclass(obj, (models.TextChoices, models.IntegerChoices)):
                    self.generate_django_enum(js_file, obj, name)
                elif issubclass(obj, enum.Enum):
                    self.generate_python_enum(js_file, obj, name)

            self.stdout.write(self.style.SUCCESS(f"✅ Enums written to: {output_path}"))

    def generate_django_enum(self, js_file, enum_class, enum_name):
        js_file.write(f"export const {enum_name} = {{\n")
        for choice in enum_class:
            js_file.write(f"    {choice.name}: {repr(choice.value)},\n")
        js_file.write("};\n\n")

        js_file.write(f"export const {enum_name}Options = [\n")
        for choice in enum_class:
            js_file.write(f"    {{ label: '{choice.label}', value: {repr(choice.value)} }},\n")
        js_file.write("];\n\n")

        js_file.write(f"export const {enum_name}Labels = {{\n")
        for choice in enum_class:
            js_file.write(f"    {repr(choice.value)}: '{choice.label}',\n")
        js_file.write("};\n\n")

    def generate_python_enum(self, js_file, enum_class, enum_name):
        js_file.write(f"export const {enum_name} = {{\n")
        for item in enum_class:
            js_file.write(f"    {item.name}: {repr(item.value)},\n")
        js_file.write("};\n\n")