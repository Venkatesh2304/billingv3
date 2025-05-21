# pdf_utils.py
from collections import namedtuple
from io import BytesIO
import re
import traceback
from concurrent.futures import ThreadPoolExecutor
import qrcode
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from PyPDF2 import PdfReader, PdfWriter
from reportlab.lib.utils import ImageReader
from app.print import PrintType 
from io import BytesIO
from PyPDF2 import PdfReader, PdfWriter
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import pymupdf

def extract_invoice_number_first_copy(page):
    """Extract the invoice number from the page."""
    text_clip = (0, 0, 600, 100)
    page_text = page.get_text("text") #, clip=text_clip)
    if "Page :\n1 of " in page_text:
        match = re.findall(r"Invoice No[ \t]*:\n.{6}", page_text)
        if match:
            return match[0][-6:]  # Return the last 6 characters as the invoice number
    return None

def extract_invoice_number_salesman_loading_sheet(page):
    """Extract the invoice number from the page."""
    text_clip = (0, 0, 600, 180)
    page_text = page.get_text("text", clip=text_clip)
    if "Page 1\n" in page_text:
        match = re.findall(r"BILL\n(.*)\n", page_text)
        if match:
            return match[0].strip() # Return the last 6 characters as the invoice number
    return None

BarcodeConfig = namedtuple("barcode_config","x y extract_invoice_fn")
configs = { 
    PrintType.FIRST_COPY : BarcodeConfig(x=180,y=760,extract_invoice_fn=extract_invoice_number_first_copy) , 
    PrintType.LOADING_SHEET_SALESMAN : BarcodeConfig(x=280,y=730,extract_invoice_fn=extract_invoice_number_salesman_loading_sheet) , 
}

def generate_aztec_code(data):
    """Generate Aztec code as a BytesIO object."""
    qr_code = qrcode.make(data, version=None, box_size=10, border=1, 
                           error_correction=qrcode.constants.ERROR_CORRECT_H)
    buffer = BytesIO()
    qr_code.save(buffer, format='PNG')
    buffer.seek(0)
    return buffer

def create_aztec_canvas(aztec_data,config:BarcodeConfig):
    """Create a PDF canvas containing the Aztec code image."""
    aztec_buffer = generate_aztec_code(aztec_data)
    img_reader = ImageReader(aztec_buffer)

    temp_pdf_buffer = BytesIO()
    temp_canvas = canvas.Canvas(temp_pdf_buffer, pagesize=A4) 
    temp_canvas.drawImage(img_reader, x=config.x, y=config.y, width=50, height=50)  # Position and size
    temp_canvas.showPage()
    temp_canvas.save()
    temp_pdf_buffer.seek(0)  # Reset buffer position for reading
    return temp_pdf_buffer

def process_pdf_page(page,config:BarcodeConfig):
    """Process each page to extract the invoice number and generate an Aztec code."""
    invoice_number = config.extract_invoice_fn(page)
    if invoice_number:
        aztec_canvas = create_aztec_canvas(invoice_number,config)
        return invoice_number, aztec_canvas
    return None, None

def add_aztec_codes_to_pdf(input_pdf_path, output_pdf_path,print_type : PrintType):
    """Add Aztec codes to a PDF based on extracted invoice numbers."""
    input_pdf_reader = PdfReader(input_pdf_path)
    input_pdf_document = pymupdf.open(input_pdf_path)
    output_pdf_writer = PdfWriter()

    # Use ThreadPoolExecutor to parallelize page processing
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(process_pdf_page, input_pdf_document[page_num], configs[print_type]): input_pdf_reader.pages[page_num]
            for page_num in range(len(input_pdf_document))
        }

        for future in futures:
            pdf_page = futures[future]
            try:
                invoice_number, aztec_buffer = future.result()
                if aztec_buffer:
                    temp_pdf_reader = PdfReader(aztec_buffer)  # Read the PDF from the BytesIO buffer
                    pdf_page.merge_page(temp_pdf_reader.pages[0])
                output_pdf_writer.add_page(pdf_page)
            except Exception as e:
                traceback.print_exc()
                print(f"Error processing page: {e}")

    # Save the final output PDF
    with open(output_pdf_path, "wb") as output_file:
        output_pdf_writer.write(output_file)

def add_image_to_pdf(pdf_path, image_path , x, y, width, height,insert_page_nums = []):

    if insert_page_nums : return pdf_path 
    temp_pdf_path = BytesIO()
    c = canvas.Canvas(temp_pdf_path, pagesize=letter)
    CM_TO_POINT = 28.35
    border = 0.05 * CM_TO_POINT
    x = x*CM_TO_POINT
    y = y*CM_TO_POINT
    w = width*CM_TO_POINT
    h = height*CM_TO_POINT
    c.rect(x - border, y - border, w + 2 * border, h + 2 * border, stroke=1, fill=0)
    c.drawImage(image_path, x, y, w, h)
    c.showPage()
    c.save()

    # Read the original PDF and the newly created PDF with the image
    original_pdf = PdfReader(pdf_path)
    pdf_writer = PdfWriter()
    image_pdf = PdfReader(temp_pdf_path)    
    image_page = image_pdf.pages[0]  # Assuming the image is only on the first page

    for num,page in enumerate(original_pdf.pages) :
        if num in insert_page_nums : page.merge_page(image_page)
        pdf_writer.add_page(page)

    output = BytesIO()
    pdf_writer.write(output)
    return output 


# Example usage
# add_aztec_codes_to_pdf("loading.pdf", "output_with_barcode.pdf")
