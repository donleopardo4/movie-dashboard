from tableauscraper import TableauScraper as TS

VIEW_URL = "https://public.tableau.com/views/PeliculasestrenadasSAF/Estrenos?:showVizHome=no"

ts = TS()
ts.loads(VIEW_URL)
wb = ts.getWorkbook()

print("Worksheets:", wb.getWorksheets())

# Probá una hoja (si 'Estrenos' no aparece, elegimos otra del listado)
sheet = "Estrenos"
df = wb.getWorksheet(sheet).data
print("Filas:", len(df), "Columnas:", list(df.columns)[:20])
print(df.head(5))
