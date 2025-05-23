#Create a billing process status model view set 
from rest_framework import viewsets,mixins
from app.models import *
from app.serializer import *
from rest_framework.response import Response
from django_filters import rest_framework as filters
from rest_framework.pagination import LimitOffsetPagination

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
                return queryset.filter(type__in = ["neft","cheque"]).annotate(pushed_bills_count=Count('ikea_collection')).filter(pushed_bills_count=0)
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
    
