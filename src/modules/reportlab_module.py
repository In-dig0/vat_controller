import pandas as pd
import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, mm
from reportlab.pdfgen.canvas import Canvas

# Classe per aggiungere numeri di pagina
class NumberedCanvas(Canvas):
    def __init__(self, *args, **kwargs):
        Canvas.__init__(self, *args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_number(num_pages)
            Canvas.showPage(self)
        Canvas.save(self)

    def draw_page_number(self, page_count):
        self.setFont("Helvetica", 9)
        self.drawRightString(
            self._pagesize[0] - 0.5*inch,
            0.5*inch,
            "Pagina %d di %d" % (self._pageNumber, page_count)
        )

def create_vat_controller_pdf(df, pdf_path, selected_columns=None, report_title="VAT CONTROLLER", cover_variables=None):
    """
    Crea un PDF di report VAT Controller con i dati forniti.
    
    Parametri:
    - df: DataFrame pandas con i dati
    - pdf_path: Percorso dove salvare il file PDF
    - selected_columns: Lista di colonne da includere nel report (se None, include tutte)
    - report_title: Titolo del report
    - cover_variables: Dizionario di variabili da mostrare nella copertina {etichetta: valore}
    """
    # Se non sono specificate colonne, usa tutte le colonne del dataframe
    if selected_columns is None:
        selected_columns = df.columns.tolist()
    
    # Usa solo le colonne selezionate
    df_selected = df[selected_columns]
    
    # Ottieni l'ora corrente
    current_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    # Crea documento PDF
    doc = SimpleDocTemplate(
        pdf_path, 
        pagesize=landscape(A4),
        rightMargin=10*mm, 
        leftMargin=10*mm,
        topMargin=10*mm, 
        bottomMargin=15*mm
    )
    
    # Stili
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        name='Title',
        parent=styles['Title'],
        fontSize=24,
        alignment=1,  # Centrato
        spaceAfter=30
    )
    subtitle_style = ParagraphStyle(
        name='Subtitle',
        parent=styles['Normal'],
        fontSize=14,
        alignment=1,  # Centrato
        spaceAfter=20
    )
    
    # Stile per le variabili in copertina
    cover_var_style = ParagraphStyle(
        name='CoverVariables',
        parent=styles['Normal'],
        fontSize=12,
        alignment=0,  # Allineato a sinistra
        leftIndent=10,  # Indentazione interna sinistra
        rightIndent=10,  # Indentazione interna destra
        spaceBefore=3,
        spaceAfter=3,
        allowWidows=0,
        allowOrphans=0
    )
    
    # Stile per il testo nelle celle
    cell_style = ParagraphStyle(
        name='CellStyle',
        parent=styles['Normal'],
        fontSize=9,
        leading=10,  # Interlinea ridotta
        wordWrap='CJK'  # Migliora il word wrapping
    )
    
    # Stile intestazione con testo BIANCO
    header_style = ParagraphStyle(
        name='HeaderStyle',
        parent=cell_style,
        fontName='Helvetica-Bold',
        textColor=colors.white,
        alignment=1  # Centrato
    )
    
    # Contenuto del documento
    elements = []
    
    # Pagina di copertina
    elements.append(Spacer(1, 2*inch))
    elements.append(Paragraph(report_title, title_style))
    elements.append(Spacer(1, 0.5*inch))
    elements.append(Paragraph(f"Data elaborazione: {current_time}", subtitle_style))
    
    # Aggiungi le variabili di copertina se presenti
    if cover_variables and isinstance(cover_variables, dict):
        elements.append(Spacer(1, 0.5*inch))
        
        # Creare una tabella per contenere le variabili di copertina
        cover_data = []
        for label, value in cover_variables.items():
            # Formatta l'etichetta in corsivo e il valore in grigio
            cover_data.append([Paragraph(f"<i>{label}</i>: <font color='#666666'>{value}</font>", cover_var_style)])
        
        # Calcola la larghezza del box
        box_width = doc.width * 0.8  # 50% della larghezza della pagina (ridotta del 30% rispetto all'80% precedente)
        
        # Crea tabella e imposta lo stile con bordo
        cover_table = Table(cover_data, colWidths=[box_width])
        cover_table.setStyle(TableStyle([
            ('BOX', (0, 0), (-1, -1), 1, colors.black),  # Bordo esterno
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('LEFTPADDING', (0, 0), (-1, -1), 12),
            ('RIGHTPADDING', (0, 0), (-1, -1), 12),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        
        elements.append(cover_table)
    
    elements.append(PageBreak())
    
    # Prepara i dati per la tabella con formattazione del testo
    # Converti i valori in Paragraph per permettere il wrapping del testo
    formatted_data = []
    
    # Intestazioni con testo BIANCO
    header_row = [Paragraph(str(col), header_style) for col in df_selected.columns]
    formatted_data.append(header_row)
    
    # Dati
    for _, row in df_selected.iterrows():
        formatted_row = [Paragraph(str(cell), cell_style) for cell in row]
        formatted_data.append(formatted_row)
    
    # Calcola le larghezze delle colonne con priorità alla colonna descrizione
    page_width = doc.width
    
    # Definisci percentuali di larghezza personalizzate per colonne specifiche
    col_percentages = {}
    
    for col in selected_columns:
        col_lower = col.lower()
        
        # Distribuzione precisa delle larghezze per ogni tipo di colonna
        if "pdesc" in col_lower or "description" in col_lower or "company" in col_lower or "name" in col_lower:
            # Colonna descrizione: 30% dello spazio totale
            col_percentages[col] = 0.30
        elif "vies_vatnr" in col_lower:
            # Colonna vies_vatnr: ampliata al 18% per evitare che vada a capo
            col_percentages[col] = 0.18
        elif "vat_number" in col_lower:
            # Altre colonne con numeri VAT: 15%
            col_percentages[col] = 0.15
        elif "ccode" in col_lower or "country_code" in col_lower:
            # Codici paese: ridotti a 5%
            col_percentages[col] = 0.05
        elif "status" in col_lower:
            # Colonne status: 8%
            col_percentages[col] = 0.08
        elif "line_nr" in col_lower:
            # Numeri di riga: 5%
            col_percentages[col] = 0.05
        elif "err_msg" in col_lower:
            # Messaggi errore: 20%
            col_percentages[col] = 0.20
        else:
            # Altre colonne: 10%
            col_percentages[col] = 0.10
    
    # Calcola le larghezze assolute in base alle percentuali
    col_widths = [page_width * col_percentages.get(col, 0.10) for col in selected_columns]
    
    # Normalizza per assicurarsi che la somma sia esattamente uguale alla larghezza disponibile
    total_width = sum(col_widths)
    col_widths = [w * (page_width / total_width) for w in col_widths]
    
    # Crea la tabella con le larghezze calcolate
    table = Table(formatted_data, colWidths=col_widths, repeatRows=1)
    
    # Definisci colore grigio chiaro per le righe alternate
    light_grey = colors.Color(0.9, 0.9, 0.9)
    
    # Aggiungi stile alla tabella
    table_style = [
        # Header
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),  # Testo BIANCO nell'intestazione
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('TOPPADDING', (0, 0), (-1, 0), 8),
        
        # Griglia per tutte le celle
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
        
        # Righe
        ('VALIGN', (0, 1), (-1, -1), 'MIDDLE'),  # Allineamento verticale al centro
        ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
        ('TOPPADDING', (0, 1), (-1, -1), 6),
    ]
    
    # Allineamento specifico per alcune colonne
    for i, col in enumerate(selected_columns):
        col_lower = col.lower()
        
        # Allineamento centrato per codici paese, numeri e status
        if "in_ccode" in col_lower or "in_vatnr" in col_lower or "vies_ccode" in col_lower or "vies_vatnr" in col_lower or "status" in col_lower or "line_nr" in col_lower:
            table_style.append(('ALIGN', (i, 1), (i, -1), 'CENTER'))
        else:
            table_style.append(('ALIGN', (i, 1), (i, -1), 'LEFT'))
    
    # Aggiungi sfondi alternati per le righe di dati
    for i in range(1, len(formatted_data)):
        if i % 2 == 0:
            table_style.append(('BACKGROUND', (0, i), (-1, i), light_grey))
    
    table.setStyle(TableStyle(table_style))
    
    elements.append(table)
    
    # Costruisci il PDF con numerazione delle pagine
    doc.build(elements, canvasmaker=NumberedCanvas)