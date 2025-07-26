#Create a billing process status model view set 
from rest_framework import viewsets,mixins
from app.models import *
from app.serializer import *
from rest_framework.response import Response
from django_filters import rest_framework as filters
from rest_framework.pagination import LimitOffsetPagination

from app.sync import sync_reports

class BillingProcessStatusViewSet(viewsets.ModelViewSet):
    queryset = BillingProcessStatus.objects.all()
    serializer_class = BillingProcessStatusSerializer
    filterset_fields = ['billing']
    ordering = ['id']

class BillingViewSet(mixins.RetrieveModelMixin,
                  viewsets.GenericViewSet):
    queryset = Billing.objects.all()
    serializer_class = BillingSerializer

class OrderViewSet(viewsets.ModelViewSet):
    queryset = Orders.objects.all()
    serializer_class = OrderSerializer
    filterset_fields = ['creditlock','place_order','billing']
    def get_queryset(self):
        qs = super().get_queryset()
        qs = qs.exclude(beat__name__contains = "WHOLE")
        return qs 
    
class BillViewSet(viewsets.ModelViewSet):
    class BillFilter(filters.FilterSet):
        bill__date = filters.DateFilter(field_name='bill__date', lookup_expr='exact')
        print_time__isnull = filters.BooleanFilter(field_name='print_time', lookup_expr='isnull')
        salesman = filters.CharFilter(method='filter_salesman')
        beat_type = filters.CharFilter(method='filter_beat')
        class Meta:
            model = Bill
            fields = []

        def filter_salesman(self, queryset, name, salesman):
            beats = list(models.Beat.objects.filter(salesman_name = salesman).values_list("name",flat=True).distinct())
            return queryset.filter(bill__beat__in = beats)

        def filter_beat(self, queryset, name, beat_type):
            if beat_type == "retail" : 
                queryset = queryset.exclude(bill__beat__contains = "WHOLESALE")
            elif beat_type == "wholesale" :
                queryset = queryset.filter(bill__beat__contains = "WHOLESALE")
            return queryset
    
    def list(self, request, *args, **kwargs):
        sync_reports(limits={"sales":5*60})
        return super().list(request, *args, **kwargs)
    
    queryset = Bill.objects.all() #[:1000]
    serializer_class = BillSerializer
    filterset_class = BillFilter
    
class Pagination(LimitOffsetPagination):
    default_limit = 300

class ChequeViewSet(viewsets.ModelViewSet):
    queryset = ChequeDeposit.objects.all()
    serializer_class = ChequeSerializer
    pagination_class = Pagination
    ordering = ["-id"]
    ordering_fields = ["id"]
    class ChequeFilter(filters.FilterSet):
        is_depositable = filters.BooleanFilter(method ='filter_is_depositable')
        def filter_is_depositable(self, queryset, name, value):
            if value : 
                return queryset.filter(deposit_date__isnull = True,cheque_date__lte = datetime.date.today())
            return queryset
    filterset_class = ChequeFilter

class BankViewSet(viewsets.ModelViewSet):
    class BankFilter(filters.FilterSet):
        date = filters.DateFilter(field_name='date', lookup_expr='exact')
        type = filters.CharFilter(field_name='type', lookup_expr='exact')
        bank = filters.CharFilter(field_name='bank', lookup_expr='exact')
        pushed = filters.BooleanFilter(method='filter_pushed')
        class Meta:
            model = BankStatement
            fields = []

        def filter_pushed(self, queryset, name, pushed):
            if pushed == False : 
                today = datetime.date.today()   
                return queryset.filter(date__gte = today - datetime.timedelta(days=30)
                                       ).filter(type__in = ["neft","cheque"]).exclude(cheque_status = "bounced").annotate(
                                pushed_bills_count=Count('ikea_collection')).filter(pushed_bills_count=0)
                # return queryset.filter(Q(collection__pushed = False) | Q(cheque_entry__collection__pushed = False)).distinct()
            return queryset 
    
    queryset = BankStatement.objects.all()
    serializer_class = BankSerializer
    filterset_class = BankFilter
    pagination_class = Pagination
    ordering = ["-date","-id"]
    
class OutstandingViewSet(viewsets.ModelViewSet):
    queryset = Outstanding.objects.filter(balance__lte = -1)
    serializer_class = OutstandingSerializer
    filterset_fields = ['beat','party']
    
class BeatViewSet(viewsets.ModelViewSet):
    queryset = Beat.objects.all()
    serializer_class = BeatSerializer
    filterset_fields = {"days" : ["icontains"]}


class TruckProductViewSet(viewsets.ModelViewSet):
    queryset = TruckProduct.objects.all()
    serializer_class = TruckProductSerializer
    pagination_class = Pagination
    ordering = ["-id"]
    filterset_fields = ["box","cbu"]
    def get_queryset(self):
        load = models.TruckLoad.objects.filter(completed = False).last()
        qs =  super().get_queryset()
        return qs.filter(load = load) if load else qs.none()