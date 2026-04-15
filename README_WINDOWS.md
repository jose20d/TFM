# TFM - Guia de ejecucion en Windows 11

Esta guia explica como ejecutar el proyecto en Windows 11 (backend ETL + API FastAPI + frontend Next.js).

## 1) Requisitos previos

Instala y valida lo siguiente:

- Windows 11 actualizado
- Python 3.10+ (recomendado 3.12)
- Node.js 20+ (incluye npm)
- PostgreSQL 14+
- PostGIS habilitado en la base de datos
- Git

Comandos de verificacion:

```powershell
python --version
node -v
npm -v
```

## 2) Preparar PostgreSQL + PostGIS

1. Instala PostgreSQL para Windows.
2. Crea una base de datos (ejemplo: `tfm_db`).
3. Con un usuario administrador, ejecuta en `psql`:

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

Debes conocer estos datos para configurar el backend:

- `DB_HOST` (normalmente `localhost`)
- `DB_PORT` (normalmente `5432`)
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

## 3) Obtener el codigo

```powershell
git clone <URL_DEL_REPO>
cd TFM
```

## 4) Levantar backend (ETL + API)

Desde la raiz del proyecto, en PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\activateEjecutar el proyecto en Windows 11
1) Requisitos previos
Instala esto antes de empezar:

Windows 11 actualizado.
Python 3.10+ (recomendado 3.12)
Verifica: python --version
Node.js 20+ (incluye npm)
Verifica: node -v y npm -v
PostgreSQL 14+
PostGIS habilitado en la base de datos
Git (para clonar/actualizar el repo)
Nota: El proyecto está probado en Linux, pero es compatible con Windows si configuras bien Python + PostgreSQL/PostGIS.

2) Preparar PostgreSQL + PostGIS
Instala PostgreSQL en Windows.
Crea una base de datos (ejemplo: tfm_db).
Abre psql con un usuario admin y ejecuta:
CREATE EXTENSION IF NOT EXISTS postgis;
Ten a mano:
Host (normalmente localhost)
Puerto (normalmente 5432)
Nombre BD
Usuario
Password
3) Clonar el proyecto
git clone <URL_DEL_REPO>
cd TFM
4) Backend (Python: ETL + API FastAPI)
En PowerShell dentro de la carpeta raíz del repo:

python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
Configura variables de entorno en la misma terminal:

$env:DB_HOST="localhost"
$env:DB_PORT="5432"
$env:DB_NAME="tfm_db"
$env:DB_USER="tu_usuario"
$env:DB_PASSWORD="tu_password"
Ejecuta ETL (descarga, limpia y carga a BD):

python main.py
Levanta API + web backend:

uvicorn web.app:app --reload
URLs:

Web backend: http://127.0.0.1:8000/
Swagger API: http://127.0.0.1:8000/docs
5) Frontend (Next.js)
En otra terminal PowerShell:

cd frontend
npm install
$env:BACKEND_API_URL="http://127.0.0.1:8001"
npm run dev
Frontend:

http://127.0.0.1:3000/
Si FastAPI está en 8000, usa:

$env:BACKEND_API_URL="http://127.0.0.1:8000"
6) Comprobación rápida
¿La API responde?
http://127.0.0.1:8000/api/v1/health
¿Frontend carga datos?
Abre http://127.0.0.1:3000/ y revisa KPIs/mapa/listas.
7) Problemas comunes en Windows
python no reconocido
Reinstala Python y marca “Add Python to PATH”.
Error de conexión a PostgreSQL
Revisa DB_*, servicio de PostgreSQL activo y credenciales.
PostGIS no habilitado
Ejecuta CREATE EXTENSION postgis; con usuario admin.
CORS o frontend sin datos
Verifica BACKEND_API_URL y que backend esté corriendo.
Dependencias npm fallan
Usa Node LTS (20+) y borra node_modules + package-lock.json si hace falta reinstalar.

$env:DB_HOST="localhost"
$env:DB_PORT="5432"
$env:DB_NAME="tfm_db"
$env:DB_USER="tu_usuario"
$env:DB_PASSWORD="tu_password"
```

Ejecuta ETL (descarga, limpia y carga en BD):

```powershell
python main.py
```

Inicia API + web backend:

```powershell
uvicorn web.app:app --reload
```

URLs utiles:

- Web backend: `http://127.0.0.1:8000/`
- API docs: `http://127.0.0.1:8000/docs`
- Health: `http://127.0.0.1:8000/api/v1/health`

## 5) Levantar frontend (Next.js)

Abre otra terminal PowerShell:

```powershell
cd frontend
npm install
```

Si backend esta en puerto `8000`:

```powershell
$env:BACKEND_API_URL="http://127.0.0.1:8000"
npm run dev
```

Si backend esta en otro puerto, ajusta `BACKEND_API_URL` segun corresponda.

Frontend:

- `http://127.0.0.1:3000/`

## 6) Validacion rapida

- El endpoint health responde en `/api/v1/health`.
- El frontend en `:3000` carga datos del backend.
- No hay errores de conexion en consola.

## 7) Problemas comunes

- `python` no reconocido: reinstala Python y marca "Add Python to PATH".
- Error de DB: revisa variables `DB_*`, credenciales y servicio PostgreSQL.
- PostGIS no habilitado: ejecuta `CREATE EXTENSION postgis;` con admin.
- Frontend sin datos: confirma `BACKEND_API_URL` y que FastAPI este arriba.
- Error de dependencias Node: usa Node LTS 20+ y reinstala `node_modules` si hace falta.

## 8) Nota de compatibilidad

El proyecto esta probado principalmente en Linux, pero puede ejecutarse en Windows 11 con la configuracion anterior.
