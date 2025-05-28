import datetime
from io import BytesIO
import pandas as pd
from fpdf import FPDF
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle , Spacer
from reportlab.lib import colors
from enum import Enum 
import pymupdf
from billingv3.settings import FILES_DIR

# Set font size and cell height
S = 10  # Font size
H = 6   # Cell height
OUTPUT_LOADING_PDF_FILE = f"{FILES_DIR}/loading.pdf"
LoadingSheetType = Enum("LoadingSheetType","Salesman Plain")

class PDF(FPDF):
    def header(self):
        # Call the parent header method if you want to keep default behavior
        super().header()

        # Move to the top right corner
        self.set_y(10)  # Adjust vertical position as needed
        # self.set_x(self.w - 30)  # Right margin minus padding
        
        # Print the page number on the right
        self.cell(0, 10, f'{datetime.date.today().strftime("%d-%m-%Y")}', 0, 0, 'L')
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'R')
        self.ln(10)

# Function to calculate the column widths based on content
def calculate_col_widths(df, pdf):
    col_widths = []
    for col in df.columns:
        max_width = pdf.get_string_width(col) + 4  # Start with the header width
        for value in df[col]:
            value_width = pdf.get_string_width(str(value).replace(' ','X')) + 4
            max_width = max(max_width, value_width)
        col_widths.append(max_width)
    scale = 190/sum(col_widths)
    col_widths = [ i*scale for i in col_widths ]
    return col_widths

# Function to print the table headers
def print_table_header(pdf, col_widths, header, S, H, B):
    pdf.set_font('Arial', '', S)
    for i, col_name in enumerate(header):
        pdf.cell(col_widths[i], H, col_name, border=B, align='L')
    pdf.ln()

# Function to print the table
def print_table(pdf,df,border = 0,print_header = True) : 
    B = border
    pdf.set_font('Arial', '', S)
    col_widths = calculate_col_widths(df, pdf)
    header = df.columns.tolist()
    if print_header:  print_table_header(pdf, col_widths, header, S, H, B)

    # Print DataFrame rows and repeat header on each new page if needed
    for index, row in df.iterrows():
        # Check if a new page is needed
        if pdf.get_y() > 280 :  # Adjust this value if you need more/less space before the footer
            pdf.add_page()      # Add a new page
            print_table_header(pdf, col_widths, header, S, H, B)  # Reprint the header on the new page

        for i, item in enumerate(row): 
                pdf.cell(col_widths[i], H, str(item), border=B, align='L')
        pdf.ln()


def loading_sheet_pdf(tables:tuple[pd.DataFrame],sheet_type:LoadingSheetType,context = {}) : 
    # Load and process the data

    df,party_sales = tables 
    df = df.dropna(subset="Sr No")
    df["MRP"] = df["MRP"].str.split(".").str[0]
    df["LC"] = df["Total LC.Units"].str.split(".").str[0]
    df["Units"] = df["Total LC.Units"].str.split(".").str[1]
    df = df.rename(columns={"Total FC": "FC", "Total Gross Sales": "Gross Value"})

    total_fc = df["FC"].iloc[-1]
    total_lc = df["LC"].iloc[-1]
    df = df.fillna("")
    df["No"] = df.index.copy() +  1    
    df[["FC","LC"]] = df[["FC","LC"]].replace({"0" : ""})
    df = df.iloc[:-1]
    df = df[["No","Product Name", "MRP", "FC", "Units", "LC","UPC", "Gross Value","Division Name"]]

    party_sales = party_sales.dropna(subset="Party")
    party_sales = party_sales.sort_values("Bill No")
    party_sales = party_sales.fillna("")
    party_sales["No"] = party_sales.reset_index(drop=True).index.copy() +  1    
    party_sales = party_sales[["No","Bill No","Party","Gross Amount","Sch.Disc","Net Amt"]]
    

    no_of_bills = len(party_sales.index) - 1 
    outlet_count = party_sales["Party"].nunique() - 1
    lines_count = len(df.index)
    # bills =  f'{party_sales["Party"].min()} - {party_sales["Party"].max()}'
    time = datetime.datetime.now().strftime("%d-%b-%Y %I:%M %p") 
    total_value = round(float(party_sales.iloc[-1]["Gross Amount"]))
    

    # Create PDF
    pdf = PDF()
    pdf.set_top_margin(15)
    pdf.set_auto_page_break(auto=True, margin=5)
    pdf.set_font('Arial', '', 10)
    pdf.add_page()
    header_table = []

    if sheet_type == LoadingSheetType.Salesman :
        pdf.cell(0, 10, "DEVAKI ENTERPRISES", 0, 0, 'L')
        pdf.ln()        
        header_table.append(["TIME",time,"","","VALUE",total_value])
        header_table.append(["SALESMAN",context["salesman"] ,"","","BEAT",context["beat"]])
        header_table.append(["PARTY",(context["party"] or "SALESMAN").ljust(34).upper(),"","","TOTAL CASE",str(int(total_fc or "0") + int(total_lc or "0"))])
        header_table.append(["BILL",context["inum"],"","","PHONE","9944833444"])
        df["Case"] = (df["FC"].apply(lambda x: int(x) if x else 0) + df["LC"].apply(lambda x: int(x) if x else 0)).astype(str).replace("0","")
        dfs = df[["No","Product Name","MRP","Case","Units","UPC","Gross Value"]]
        dfs.loc[len(dfs.index)] = ["","Total"] + [""] * 4 + [total_value]
        
    if sheet_type == LoadingSheetType.Plain :
        header_table.append(["TIME",time,"","","BILLS",no_of_bills])
        header_table.append(["LINES",lines_count,"","","OUTLETS",outlet_count])
        header_table.append(["TOTAL LC",total_lc,"","","TOTAL FC",total_fc])
        df[["LC.","Units.","FC."]] = df[["LC","Units","FC"]].copy()
        df['group'] = (df['Division Name'] != "").cumsum()
        split_dfs = [group for _, group in df.groupby('group') if (group['Division Name'] != "").any()]
        dfs = [group[["No","Product Name","MRP","LC","Units","FC","UPC","LC.","Units.","FC."]] for group in split_dfs]


    header_table = pd.DataFrame(header_table,dtype="str",columns=["a","b","c","d","e","f"])
    print_table(pdf,header_table,border=0,print_header=False)
    pdf.ln(5)
    if type(dfs) == pd.DataFrame : dfs = [dfs,]
    for index,df in enumerate(dfs) :
        print_table(pdf,df,border=1)
        if index < len(dfs) -1 : 
            pdf.ln(25)

    if sheet_type == LoadingSheetType.Plain : 
        pdf.add_page()
    if sheet_type == LoadingSheetType.Salesman : 
        pdf.ln(5)

    print_table(pdf,party_sales,border = 1)
        
    # Output the PDF
    pdf.output(OUTPUT_LOADING_PDF_FILE)

    print(f"PDF generated: {OUTPUT_LOADING_PDF_FILE}")

def pending_sheet_pdf(df, sheet_no ,salesman,beat,date):
    bytesio = BytesIO()
    # Define the PDF document with specified margins
    pdf = SimpleDocTemplate(bytesio, pagesize=letter, leftMargin=30, rightMargin=30, topMargin=10, bottomMargin=10)
    
    # Calculate the width of the page and the columns
    width, height = letter
    total_width = width - 60  # Subtract margins

    header_data = [[sheet_no, salesman],[beat,date.strftime("%d-%b-%Y")]]
    header_table = Table(header_data, colWidths=[total_width * 0.5, total_width * 0.5])
    header_table.setStyle(TableStyle([
        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
        ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LINEBELOW', (0, 1), (-1, 1), 1, colors.black) , 
        ('BOTTOMPADDING', (0,1), (-1,1), 10),
    ]))


    first_column_width = total_width * 0.3  # 30% of the content width for the first column
    remaining_column_width = (total_width - first_column_width) / 5  # Divide remaining space among other columns

    df = df.rename(columns = {"Bill Net Amt":"Bill","Collected Amount":"Coll","OutstANDing Amount":"Outstanding","Bill Ageing (In Days)":"Days","Sr No":" "})
    df["Date"] = df["Date"].dt.strftime("%d/%m/%Y")
    for col in ["Coll","Outstanding","Bill"] : 
        df[col] = df[col].astype(str).str.split(".").str[0]
    data = []
    for _,row in df.iterrows() : 
        days = str(row["Days"]).split(".")[0]
        data.append([ row["Party Name"].split("-")[0][:27] , row["Date"] , row["Salesperson Name"][:12] , days , " " , " " ])
        data.append([ row["Bill No"] + " "*9 + days + " days" , row["Bill"] , row["Coll"] , row["Outstanding"] , " " , " " ])


    # Create the table and specify column widths
    table = Table(data, colWidths= [total_width*0.3] + [total_width*0.12,total_width*0.15,total_width*0.1,total_width*0.13]  + [total_width*0.20])
    
    # Initialize the table style with basic configurations
    table_style = TableStyle([
        ('BOTTOMPADDING', (0,0), (-1,-1), 2),
        ('TOPPADDING', (0,0), (-1,-1), 2),
        ('LEFTPADDING', (0,0), (-1,-1), 5),
        ('RIGHTPADDING', (0,0), (-1,-1), 5),
        ('FONT', (0,0), (-1,-1), 'Helvetica', 10),
        ('TEXTCOLOR', (0,0), (-1,-1), colors.black),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('LINEBEFORE', (4, 0), (5, -1), 1, colors.black) ,
    ])

    # Apply a bottom border only to even rows (2, 4, 6, ...)
    for row_index in range(1, len(data), 2):  # Start at 1 and step by 2
        table_style.add('LINEBELOW', (0, row_index), (-1, row_index), 1, colors.black)

    table.setStyle(table_style)
    total_outstanding = round(df["Outstanding"].astype(float).sum())
    count_table = [("Bills",len(df.index)),("Return"," "),
                   ("Out Amt",total_outstanding),("Coll Amt"," ")]
    denomination_data1 = [(500,"","") , (200,"","") , (100,"","") , (50,"","") ] 
    denomination_data2 = [(20,"","") , (10,"","") ,("Coins","",""),("Total","","")] 
    common_style = TableStyle([ ('GRID', (0, 0), (-1, -1), 1, colors.black) , ('TOPPADDING',(0,0),(-1,-1),20) ])
    widths = [total_width/15,total_width/10,total_width/4]
    c = Table(count_table, colWidths=[total_width/10,total_width/10],style=common_style)
    d1 = Table(denomination_data1, colWidths=widths,style=common_style)
    d2 = Table(denomination_data2, colWidths=widths,style=common_style)
    combined_table = [[c , d1 , d2]]
    combined_table = Table(combined_table)

    elements = [header_table,table,Spacer(1, 20),combined_table] #Paragraph(sheet_no), Paragraph() , 
    pdf.build(elements)
    return bytesio 

def remove_blank_pages_from_first_copy(pdf_path, blank_threshold=640):
    doc = pymupdf.open(pdf_path)
    output_pdf = pymupdf.open()  # Create a new PDF document

    for page_num in range(len(doc)):
        page = doc[page_num]
        page_height = page.rect.height  # Total height of the page
        text_instances = page.get_text("dict")["blocks"]

        max_y = 0  # Track the maximum Y-coordinate of text

        for block in text_instances:
            if "bbox" in block:  # Each block has a bounding box
                y1 = block["bbox"][3]  # Bottom Y-coordinate
                if y1 > max_y:
                    max_y = y1

        # Calculate blank height
        blank_height = page_height - max_y

        # Check if the blank height exceeds the threshold
        if blank_height < blank_threshold:
            output_pdf.insert_pdf(doc, from_page=page_num, to_page=page_num)

    output_pdf.save(pdf_path)
    output_pdf.close()
    doc.close()
