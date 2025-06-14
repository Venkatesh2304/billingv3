# Generated by Django 5.1.1 on 2025-06-08 12:42

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("app", "0006_alter_bankstatement_bank"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="truckproduct",
            name="barcode",
        ),
        migrations.AddField(
            model_name="purchaseproduct",
            name="mrp",
            field=models.IntegerField(default=0),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="truckproduct",
            name="id",
            field=models.BigAutoField(
                auto_created=True,
                primary_key=True,
                serialize=False,
                verbose_name="ID",
            ),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="truckproduct",
            name="qty",
            field=models.IntegerField(default=0),
            preserve_default=False,
        ),
    ]
