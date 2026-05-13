# TFM - Guía de ejecución en Windows 11

Esta guía describe la ejecución del proyecto en Windows 11 (ETL + API FastAPI + frontend Next.js).

## 1) Requisitos previos

- Windows 11 actualizado
- Python 3.10+ (recomendado 3.12)
- Node.js 20+ (incluye npm)
- PostgreSQL 14+
- PostGIS habilitado en la base de datos
- Git

Verificación:

```powershell
python --version
node -v
npm -v
```

## 2) Preparar PostgreSQL + PostGIS

1. Instala PostgreSQL para Windows.
2. Crea una base de datos (ejemplo: `tfm_db`).
3. Con un usuario administrador, ejecuta:

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

Ten a mano:

- `DB_HOST` (normalmente `localhost`)
- `DB_PORT` (normalmente `5432`)
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

## 3) Obtener el código

```powershell
git clone <URL_DEL_REPO>
cd TFM
```

## 4) Backend (ETL + API)

En PowerShell, desde la raíz del repo:

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

Configura variables de entorno:

```powershell
$env:DB_HOST="localhost"
$env:DB_PORT="5432"
$env:DB_NAME="tfm_db"
$env:DB_USER="tu_usuario"
$env:DB_PASSWORD="tu_password"
```

Ejecuta ETL:

```powershell
python main.py
```

Levanta backend:

```powershell
uvicorn web.app:app --reload --port 8000
```

URLs útiles:

- Web backend: `http://127.0.0.1:8000/`
- API docs: `http://127.0.0.1:8000/docs`
- Health: `http://127.0.0.1:8000/api/v1/health`

## 5) Frontend (Next.js)

En otra terminal:

```powershell
cd frontend
npm install
$env:BACKEND_API_URL="http://127.0.0.1:8000"
npm run dev
```

Frontend:

- `http://127.0.0.1:3000/`

## 6) Validación rápida

- `http://127.0.0.1:8000/api/v1/health` responde.
- `http://127.0.0.1:3000/` carga datos.
- No hay errores de conexión en consola.

## 7) Problemas comunes

- `python` no reconocido: reinstala Python y marca "Add Python to PATH".
- Error de DB: revisa `DB_*`, credenciales y servicio PostgreSQL activo.
- PostGIS no habilitado: ejecuta `CREATE EXTENSION postgis;` con admin.
- Frontend sin datos: confirma `BACKEND_API_URL` y backend arriba.
- Dependencias npm: usa Node LTS 20+ y reinstala `node_modules` si hace falta.

## 8) Nota de compatibilidad

El proyecto se prueba principalmente en Linux, pero puede ejecutarse en Windows 11 con esta configuración.
