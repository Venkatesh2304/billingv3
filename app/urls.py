from app.billing import billing_view
from app.print import print_bills
from app import bank, report ,load , bill_check , stock_check
from . import views
from django.urls import path
from django.views.decorators.cache import cache_page
from rest_framework import routers
from app.views import *
from app.modelviews import * 
from django.conf import settings
from django.conf.urls.static import static
from django.contrib.auth import views as auth_views
from django.urls import re_path, include

router = routers.DefaultRouter()
router.register(r'billing_status', BillingProcessStatusViewSet)
router.register(r'billing', BillingViewSet)
router.register(r'order', OrderViewSet)
router.register(r'bill', BillViewSet)
router.register(r'cheque', ChequeViewSet)
router.register(r'bank', BankViewSet)
router.register(r'outstanding', OutstandingViewSet)
router.register(r'beat', BeatViewSet)
router.register(r'truckproduct', TruckProductViewSet)

urlpatterns = [
    path('', include(router.urls)),

    path('start_billing/', billing_view , name='start_billing'),
    path('print_bills/', print_bills , name='print_bills'),

    path('download/<str:fname>/', views.download_file, name='download_file'),
    path('einvoice_login/', views.einvoice_login, name='einvoice_login'),
    path('salesman/', views.salesman_names, name='salesman'),
    path('party/', views.party_names, name='party'),
    path('einvoice_status/', views.einvoice_status, name='einvoice_status'),

    path('cheque_match/<str:bank_id>/', bank.cheque_match, name='cheque_match'),
    path('bank_collection/<str:bank_id>/', bank.bank_collection, name='bank_collection'),
    path('deposit_slip/', bank.generate_deposit_slip, name='deposit_slip'),
    path('bank_statement_upload/', bank.bank_statement_upload, name='bank_statement_upload'),
    path('push_collection/', bank.push_collection, name='push_collection'),
    path('unpush_collection/<str:bank_id>/', bank.unpush_collection, name='unpush_collection'),
    path('match_upi/', bank.auto_match_upi, name='match_upi'),
    path('match_neft/', bank.auto_match_neft, name='match_neft'),
    path('refresh_bank/', bank.refresh_bank, name='refresh_bank'),


    path('basepack/', report.basepack , name='basepack'),
    path('outstanding_report/', report.outstanding , name='outstanding_report'),
    path('pending_sheet/', report.pending_sheet , name='pending_sheet'),
    path('stock_statement/', report.stock_statement , name='stock_statement'),

    path('upload_purchase/', load.upload_purchase_invoice , name='upload_purchase'),
    path('get_last_load/', load.get_last_load , name='get_last_load'),
    path('map_load/', load.map_purchase_to_load , name='map_purchase_to_load'),
    path('get_product/', load.get_product , name='get_product'),
    path('get_cbu_codes/', load.get_cbu_codes , name='get_cbu_codes'),
    path('finish_load/', load.finish_load , name='finish_load'),
    path('load_summary/', load.load_summary , name='load_summary'),
    path('scan_product/', load.scan_product , name='scan_product'),
    path('get_box_no/', load.get_box_no , name='get_box_no'),

    path('get_bill_products/', bill_check.get_bill_products , name='get_bill_products'),
    path('get_closing_products/', stock_check.get_closing_products , name='get_closing_products'),
    path('get_product_from_barcode/', bill_check.get_product_from_barcode , name='get_product_from_barcode'),



    # path('party_outstanding/', views.get_party_outstanding, name='party_outstanding'),
    # path('billautocomplete/', BillAutocomplete.as_view(), name='billautocomplete'),
    # path('reload_server/', reload_server_view , name='reload_server'),
    # path('manual_print/', manual_print_view , name='manual-print'),
    # path('get_bill_data/', get_bill_data, name='get_bill_data'),
    # path('get_party_outstanding/', get_party_outstanding, name='get_party_outstanding'),
    # path('salesman_cheque/', salesman_cheque_entry_view, name='salesman_cheque'),
    # path('add_salesman_cheque', add_salesman_cheque, name='add_salesman_cheque'),
    # path('scan_bills', scan_bills, name='scan_bills'),
    # path('scan_pending_bills', ScanPendingBills.as_view() , name='scan_pending_bills'),
    # path('sync_impact', sync_impact, name='sync_impact'),
    # # path('vehicle_selection', vehicle_selection, name='vehicle_selection'),
    # path('get_bill_out', get_bill_out, name='get_bill_out'),
    # path('get_bill_in', get_bill_in, name='get_bill_in'),
    # path('party-sync', basepack, name='basepack'),
    # path('jsi18n/', cache_page(3600)(admin_site.i18n_javascript), name='javascript-catalog'),
    # path('', admin_site.urls) ,
]