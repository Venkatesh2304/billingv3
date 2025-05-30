# Generated by Django 5.1.1 on 2025-05-29 11:22

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("app", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="TruckLoad",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("date", models.DateField(auto_now_add=True)),
                ("completed", models.BooleanField(db_default=False, default=False)),
            ],
        ),
        migrations.CreateModel(
            name="TruckProduct",
            fields=[
                (
                    "barcode",
                    models.CharField(max_length=300, primary_key=True, serialize=False),
                ),
                ("cbu", models.CharField(max_length=20)),
            ],
        ),
        migrations.CreateModel(
            name="PurchaseProduct",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("inum", models.CharField(max_length=30)),
                ("cbu", models.CharField(max_length=20)),
                ("sku", models.CharField(max_length=20)),
                ("qty", models.IntegerField()),
                (
                    "load",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        related_name="purchase_products",
                        to="app.truckload",
                    ),
                ),
            ],
        ),
    ]
