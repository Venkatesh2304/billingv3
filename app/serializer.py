# Create a serialiser for BillingProcess
import datetime
from rest_framework import serializers
from app.models import *
from rest_framework import serializers
from django.utils import timezone
from django.db import transaction
from django.db.models import F
import app.models as models
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _
from django.db.models import Max, F, Subquery, OuterRef, Q, Min, Sum, Count
from app.billing import BillingStatus
from drf_writable_nested import WritableNestedModelSerializer

class BillingProcessStatusSerializer(serializers.ModelSerializer):
    class Meta:
        model = BillingProcessStatus
        fields = ["process", "status", "time"]

class OrderSerializer(serializers.ModelSerializer):
    party = serializers.CharField(source="party.name", read_only=True)
    beat = serializers.CharField(source="beat.name", read_only=True)
    potential_release = serializers.SerializerMethodField()

    class Meta:
        model = Orders
        fields = [
            "order_no",
            "party",
            "lines",
            "bill_value",
            "OS",
            "coll",
            "salesman",
            "beat",
            "phone",
            "type",
            "cheque",
            "potential_release",
        ]

    def get_potential_release(self, order):
        if not order.place_order : return False 
        today = datetime.date.today()
        outstanding_qs = models.Outstanding.objects.filter(
            party=order.party, beat=order.beat.name, balance__lte=-1
        )
        today_bill_count = models.Sales.objects.filter(
            party=order.party, beat=order.beat.name, date=today
        ).count()
        if (today_bill_count == 0) and (outstanding_qs.count() == 1):
            bill_value = order.bill_value
            outstanding_bill: models.Outstanding = outstanding_qs.first()  # type: ignore
            outstanding_value = -outstanding_bill.balance
            if bill_value < 200:
                return False

            max_outstanding_day = (today - outstanding_bill.date).days
            max_collection_day = models.Collection.objects.filter(
                party=order.party, date=today
            ).aggregate(date=Max("bill__date"))["date"]
            max_collection_day = (
                (today - max_collection_day).days if max_collection_day else 0
            )
            if (max_collection_day > 21) or (max_outstanding_day > 21):
                return False
            if (bill_value <= 500) or (outstanding_value <= 500):
                return True

class BillingSerializer(serializers.ModelSerializer):

    stats = serializers.SerializerMethodField()

    class Meta:
        model = Billing
        fields = ["stats"]

    def get_stats(self, obj):
        today = datetime.date.today()

        today_stats = (
            models.Sales.objects.filter(date=today, type="sales")
            .exclude(beat__contains="WHOLE")
            .aggregate(
                bill_count=Count("inum"),
                start_bill_no=Min("inum"),
                end_bill_no=Max("inum"),
            )
        )

        today_stats |= models.Billing.objects.filter(start_time__gte=today).aggregate(
            success=Count("status", filter=Q(status=BillingStatus.Success)),
            failures=Count("status", filter=Q(status=BillingStatus.Failed)),
        )

        # obj = models.Billing.objects.filter(start_time__gte = today).order_by("-start_time").first() or models.Billing(status = BillingStatus.NotStarted,id=-1)

        orders_qs = models.Orders.objects.filter(billing=obj).exclude(
            beat__name__contains="WHOLE"
        )
        # orders_qs = orders_qs.exclude(
        #     products__allocated=F("products__quantity")
        # ).distinct()

        stats = {
            "today": {
                "TODAY BILLS COUNT": today_stats["bill_count"],
                "TODAY BILLS": f'{today_stats["start_bill_no"]} - {today_stats["end_bill_no"]}',
                "SUCCESS": today_stats["success"],
                "FAILURES": today_stats["failures"],
            },
            "last": {
                "LAST BILLS COUNT": obj.bill_count
                or "-",  # "LAST COLLECTION COUNT" : last_billing.collection.count() if last_billing.pk else "-" ,
                "LAST BILLS": f'{obj.start_bill_no or ""} - {obj.end_bill_no or ""}',
                "LAST STATUS": BillingStatus(obj.status).name.upper(),
                "LAST TIME": f'{obj.start_time.strftime("%H:%M:%S") if obj.start_time else "-"}',
                "LAST REJECTED": orders_qs.filter(place_order=False).count(),
                "LAST PENDING": orders_qs.filter(
                    place_order=True, creditlock=False
                ).count(),
            },
            "bill_counts": {
                "rejected": orders_qs.filter(place_order=False).count(),
                "pending": orders_qs.filter(place_order=True, creditlock=False).count(),
                "creditlock": orders_qs.filter(
                    place_order=True, creditlock=True
                ).count(),
            },
        }
        return stats

class BillSerializer(serializers.ModelSerializer):
    date = serializers.SlugField(source="bill.date", read_only=True)
    amt = serializers.SlugField(source="bill.amt", read_only=True)
    party = serializers.SlugField(source="bill.party.name", read_only=True)
    beat = serializers.SlugField(source="bill.beat", read_only=True)
    einvoice = serializers.SerializerMethodField()
    delivered = serializers.BooleanField(source="bill.delivered", read_only=True)
    class Meta:
        model = Bill
        fields = ["bill", "party", "date", "salesman", "beat","amt","print_time","print_type","einvoice","delivered"]

    def get_einvoice(self, obj):
        return bool(obj.bill.ctin is None) or bool(obj.irn)

class BankCollectionSerializer(serializers.ModelSerializer):
    balance = serializers.SerializerMethodField()
    party = serializers.SlugField(source="bill.party.name", read_only=True)
    class Meta:
        model = BankCollection
        fields = ["balance", "amt", "bill","party"]

    def get_balance(self, obj):
        return -obj.bill.balance

class OutstandingSerializer(serializers.ModelSerializer):
    balance = serializers.SerializerMethodField()
    days = serializers.SerializerMethodField()
    bill = serializers.SlugField(source="inum", read_only=True)
    party = serializers.SlugField(source="party.name", read_only=True)
    class Meta:
        model = Outstanding
        fields = ["balance", "bill" , "days","party"]
    
    def get_balance(self, obj):
        return round(abs(-obj.balance))
    
    def get_days(self,obj) : 
        return (datetime.date.today() - obj.date).days
    

class ChequeSerializer(WritableNestedModelSerializer):
    party = serializers.PrimaryKeyRelatedField(queryset=Party.objects.all())
    collection = BankCollectionSerializer(many=True)
    party_name = serializers.SlugField(source="party.name", read_only=True)
    bank_entry = serializers.SlugField(source="bank_entry.id", read_only=True)
    class Meta:
        model = ChequeDeposit
        fields = ["id","cheque_date","cheque_no","party_name","amt","bank","deposit_date",
                            "collection","party","bank_entry"]

class BankSerializer(WritableNestedModelSerializer):
    collection = BankCollectionSerializer(many=True) #Only Neft Collection
    class Meta:
        model = BankStatement
        fields = ["date","ref","desc","amt","bank","status","pushed","type","id",
                    "cheque_entry","cheque_status","collection"]
        # readonly_fields = ["amt","desc","date","ref","bank","idx","id"]
        # list_filter = ["date","type","bank"]


class BeatSerializer(serializers.ModelSerializer) : 
      class Meta:
        model = Beat
        fields = ["id", "name", "salesman_name","days","plg"]
 

class TruckProductSerializer(serializers.ModelSerializer):
    class Meta:
        model = TruckProduct
        fields = ["id","box", "cbu", "qty","load"]


# class LoadSerializer(WritableNestedModelSerializer):
#     class TruckPurchaseSerializer(serializers.Serializer):
#         class Meta:
#             model = TruckPurchase
#             fields = ['inum']
#     purchase = TruckPurchaseSerializer(many=True, read_only=False)
#     class Meta:
#         model = TruckLoad
#         fields = [""]