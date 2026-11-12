# DEMO-001 · Lakehouse gobernado

Demostrador visual estático con métricas agregadas de la instantánea sintética DEMO-001. El contenido es evidencia fija, no una aplicación conectada ni datos en tiempo real. No realiza llamadas cloud, no usa SQL Warehouse ni Power BI y no requiere instalación de paquetes.

## Abrir en Windows

1. Abre PowerShell o Terminal en la carpeta `demo` (la que contiene `index.html`). Desde la raíz del repositorio:

   ```powershell
   cd demo
   ```

2. Inicia el servidor estático de Python:

   ```powershell
   py -m http.server 8000 --bind 127.0.0.1
   ```

3. Abre `http://127.0.0.1:8000/` en el navegador. Para detener el servidor, vuelve a la terminal y pulsa `Ctrl+C`.

No hace falta `npm install`. Los datos mostrados están escritos en la página y son una instantánea estática; el rótulo de verificación es una referencia de evidencia, no una consulta en directo.

En la sección de resultados, usa los botones **Gold publicado** y **Evidencia de calidad** para cambiar de vista. Funcionan con teclado y anuncian la selección a lectores de pantalla.
