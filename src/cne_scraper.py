import asyncio
import json
import logging
import os
import random
import time
from datetime import datetime

import aiofiles
import aiohttp
import aiosqlite
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()


class CNE_Scraper_Async:
    def __init__(self):
        # --- CONFIGURATION ---
        # Define base directories relative to the script location (src/)
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.project_root = os.path.dirname(
            self.base_dir
        )  # Go up one level to project root

        self.log_file = os.path.join(self.base_dir, "cne_async_audit.log")

        # Database Path: data/databases/
        self.db_file = os.path.join(
            self.project_root, "data", "databases", "HND_2025_Presidential_Results.db"
        )

        # --- CONCURRENCY ---
        self.NUM_WORKERS = 4  # Simultaneous workers

        self.setup_logging()

        # Base Headers
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://resultadosgenerales2025.cne.hn/",
            "Origin": "https://resultadosgenerales2025.cne.hn",
            "Content-Type": "application/json",
            "Cookie": os.getenv("CNE_COOKIE", ""),
        }

        # Endpoints
        self.BASE_API = "https://resultadosgenerales2025-api.cne.hn/esc/v1"
        self.NIVEL = "01"  # Presidential level
        self.URL_VOTOS = f"{self.BASE_API}/presentacion-resultados/votos"

        # Output Directories
        # PDF Path: assets/scans/pdf/
        self.pdf_dir = os.path.join(self.project_root, "assets", "scans", "pdf")

        # Image Path: assets/party_logos/
        self.img_folder = os.path.join(self.project_root, "assets", "party_logos")

        # Ensure directories exist
        os.makedirs(self.pdf_dir, exist_ok=True)
        os.makedirs(self.img_folder, exist_ok=True)

        # Ensure database directory exists
        os.makedirs(os.path.dirname(self.db_file), exist_ok=True)

    def setup_logging(self):
        """Sets up logging configuration."""
        logging.basicConfig(
            filename=self.log_file,
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            encoding="utf-8",
        )
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(message)s")
        console.setFormatter(formatter)
        logging.getLogger("").addHandler(console)

    async def init_db(self):
        """Initializes the database with WAL mode for high concurrency."""
        async with aiosqlite.connect(self.db_file) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS scraped_data (
                    id_jrv INTEGER PRIMARY KEY,
                    depto_nom TEXT,
                    muni_nom TEXT,
                    centro_nom TEXT,
                    estado_acta TEXT,
                    votos_validos_calculados INTEGER,
                    json TEXT,
                    updated_at TIMESTAMP
                )
            """)
            await db.commit()

    async def verify_db_existance(self, id_jrv):
        """Quick check (Index Scan) to see if the record already exists."""
        async with aiosqlite.connect(self.db_file) as db:
            cursor = await db.execute(
                "SELECT 1 FROM scraped_data WHERE id_jrv = ?", (id_jrv,)
            )
            resultado = await cursor.fetchone()
            return resultado is not None

    async def fetch(self, session, method, url, json_payload=None, retries=3):
        """Standard fetch method for data tasks with retry logic."""
        for attempt in range(retries):
            try:
                # Microscopic jitter to avoid exact network signatures
                await asyncio.sleep(random.uniform(0.4, 0.8))

                if method == "GET":
                    async with session.get(
                        url, headers=self.headers, timeout=15
                    ) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status in [404, 204]:
                            return None
                        else:
                            logging.warning(f"GET {response.status} at {url}")
                elif method == "POST":
                    async with session.post(
                        url, json=json_payload, headers=self.headers, timeout=15
                    ) as response:
                        if response.status == 200:
                            return await response.json()
                        # Note: Sometimes POST returns 204 if no data, treat as None
                        elif response.status == 204:
                            return None
                        else:
                            logging.warning(f"POST {response.status} at {url}")
            except Exception as e:
                logging.error(f"Error {method} {url} (Attempt {attempt + 1}): {e}")
                await asyncio.sleep(1 * (attempt + 1))
        return None

    async def fetch_navigation_robust(self, session, url, context_msg):
        """Specialized fetch for navigation (Departments/Municipalities)."""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                data = await self.fetch(session, "GET", url, retries=1)
                if data is not None:
                    return data
                logging.warning(
                    f"Empty navigation at {context_msg} (Attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(2 * (attempt + 1))
            except Exception as e:
                logging.error(f"Navigation failed {context_msg}: {e}")
                await asyncio.sleep(2)
        logging.critical(f"❌ FINAL FAILURE: Skipping branch {context_msg}.")
        return []

    async def descargar_asset(self, session, url, folder, filename):
        """Downloads a file (image/pdf) to the specified folder."""
        if not url:
            return None
        full_path = os.path.join(folder, filename)

        # Skip if file exists and is not empty
        if os.path.exists(full_path) and os.path.getsize(full_path) > 0:
            return filename
        try:
            async with session.get(url, timeout=20) as r:
                if r.status == 200:
                    content = await r.read()
                    async with aiofiles.open(full_path, mode="wb") as f:
                        await f.write(content)
                    return filename
        except Exception:
            return None
        return None

    async def procesar_mesa_task(self, session, mesa, ids_geo, noms_geo):
        """Main processing logic for a single polling station (Mesa/JRV)."""
        id_jrv = mesa.get("numero") or mesa.get("jrv")
        if not id_jrv:
            return
        id_str = str(id_jrv)

        # Skip if already in DB
        if await self.verify_db_existance(int(id_str)):
            return

        # Prepare Base Payload
        payload_base = {
            "codigos": [],
            "tipco": self.NIVEL,
            "mesa": int(id_str),
            "depto": ids_geo["depto"],
            "mcpio": ids_geo["muni"],
            "zona": ids_geo["zona"],
            "pesto": ids_geo["centro"],
            "comuna": "00",
        }

        # Specific Payloads
        payload_blancos = payload_base.copy()
        payload_blancos["codigos"] = ["996"]

        payload_nulos = payload_base.copy()
        payload_nulos["codigos"] = ["997", "998"]

        # --- CRITICAL FIX: PARTIAL SEQUENTIALITY ---
        # Separate requests to avoid saturating the server with simultaneous hits.

        # Group 1: Main Data (Parallel)
        urls_main = [
            (f"{self.BASE_API}/presentacion-resultados/actas-validas", payload_base),
            (f"{self.BASE_API}/presentacion-resultados/sufragantes", payload_base),
            (f"{self.BASE_API}/presentacion-resultados", payload_base),
        ]
        resps_main = await asyncio.gather(
            *[self.fetch(session, "POST", u, p) for u, p in urls_main]
        )
        data_val, data_suf, data_res = resps_main

        # Short pause before requesting special votes
        await asyncio.sleep(0.1)

        # Group 2: Blank Votes (Individual)
        data_blancos = await self.fetch(
            session, "POST", self.URL_VOTOS, payload_blancos
        )

        # Group 3: Null Votes (Individual)
        data_nulos = await self.fetch(session, "POST", self.URL_VOTOS, payload_nulos)

        # Validation checks
        data_val = data_val or {}
        data_suf = data_suf or {}
        data_res = data_res or {}

        # Process Special Votes
        def extraer_votos(resp):
            if resp is None:
                return 0

            if isinstance(resp, (int, float)):
                return int(resp)

            total = 0
            if isinstance(resp, list):
                for item in resp:
                    total += item.get("votos", 0)
            elif isinstance(resp, dict):
                # Case: Direct dictionary or with 'resultados' key
                if "resultados" in resp:
                    for item in resp["resultados"]:
                        total += item.get("votos", 0)
                elif "votos" in resp:
                    total += resp.get("votos", 0)
            return total

        votos_blancos = extraer_votos(data_blancos)
        votos_nulos = extraer_votos(data_nulos)

        # Process Candidates
        lista_candidatos = []
        tasks_imagenes = []
        if "candidatos" in data_res:
            for cand in data_res["candidatos"]:
                p_id = cand.get("parpo_id", "0")
                c_cod = cand.get("cddto_codigo", "000")
                f_partido = f"Partido_{p_id}.png"
                f_candidato = f"Cand_{p_id}_{c_cod}.png"

                tasks_imagenes.append(
                    self.descargar_asset(
                        session, cand.get("parpo_link_logo"), self.img_folder, f_partido
                    )
                )
                tasks_imagenes.append(
                    self.descargar_asset(
                        session,
                        cand.get("cddto_link_logo"),
                        self.img_folder,
                        f_candidato,
                    )
                )

                lista_candidatos.append(
                    {
                        "id_partido_string": p_id,
                        "id_partido_int": cand.get("parpo_id_int", 0),
                        "id_candidato": c_cod,
                        "nombre_candidato": cand.get("cddto_nombres"),
                        "nombre_partido": cand.get("parpo_nombre"),
                        "votos": cand.get("votos", 0),
                        "color_hex": cand.get("parpo_color"),
                        "imgs_locales": {
                            "partido": f_partido,
                            "candidato": f_candidato,
                        },
                    }
                )

        if tasks_imagenes:
            await asyncio.gather(*tasks_imagenes)

        # Calculations and PDF Download
        total_validos = sum(c["votos"] for c in lista_candidatos)
        total_urna = total_validos + votos_blancos + votos_nulos

        url_pdf = mesa.get("nombre_archivo")
        pdf_local = None
        if url_pdf:
            pdf_name = f"HND_2025_JRV_{int(id_str):05d}.pdf"
            # Save PDF to 'scans/pdf' directory
            await self.descargar_asset(session, url_pdf, self.pdf_dir, pdf_name)
            if os.path.exists(os.path.join(self.pdf_dir, pdf_name)):
                pdf_local = pdf_name

        # Construct Final JSON
        json_final = {
            "id_jrv": int(id_str),
            "timestamps": {
                "extraccion_local": datetime.now().isoformat(),
                "corte_servidor": data_res.get("fecha_corte"),
            },
            "geografia": {
                "cod_depto": ids_geo["depto"],
                "nom_depto": noms_geo["depto"],
                "cod_muni": ids_geo["muni"],
                "nom_muni": noms_geo["muni"],
                "cod_zona": ids_geo["zona"],
                "nom_zona": noms_geo["zona"],
                "cod_centro": ids_geo["centro"],
                "nom_centro": noms_geo["centro"],
            },
            "auditoria": {
                "estado_global": "Publicada"
                if data_val.get("publicadas") == 1
                else "No Publicada",
                "es_correcta": data_val.get("correctas", 0),
                "tiene_inconsistencias": data_val.get("inconsistencias", 0),
                "en_verificacion": data_val.get("verificacion", 0),
                "pend_verif_visual": data_val.get("pendientesVerificacionVisual", 0),
                "en_espera": data_val.get("espera", 0),
            },
            "estadisticas": {
                "censo_mesa": data_suf.get("sufragantes", 0),
                "total_firmas": data_suf.get("cantidadDeFirmas", 0),
                "participacion_pct": data_suf.get("participacion", 0),
                "votos_validos_calc": total_validos,  # Nombre campo v2
                "votos_blancos": votos_blancos,  # Restaurado
                "votos_nulos": votos_nulos,  # Restaurado
                "total_votos_calculados": total_urna,  # Nombre campo v2
            },
            "detalle_resultados": lista_candidatos,
            "documentos": {
                "pdf_descargado": pdf_local is not None,
                "pdf_nombre_local": pdf_local,
                "url_origen": url_pdf.split("?")[0] if url_pdf else None,
                "url_tokenizada": url_pdf,
            },
        }

        # Save to Database
        try:
            async with aiosqlite.connect(self.db_file) as db:
                await db.execute(
                    """
                    INSERT OR REPLACE INTO scraped_data 
                    (id_jrv, depto_nom, muni_nom, centro_nom, estado_acta, votos_validos_calculados, json, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        int(id_str),
                        noms_geo["depto"],
                        noms_geo["muni"],
                        noms_geo["centro"],
                        json_final["auditoria"]["estado_global"],
                        total_validos,
                        json.dumps(json_final, ensure_ascii=False),
                        datetime.now(),
                    ),
                )
                await db.commit()
            logging.info(f"OK JRV {id_str})")
        except Exception as e:
            logging.error(f"Error DB JRV {id_str}: {e}")

    async def worker(self, queue, session):
        while True:
            try:
                item = await queue.get()
                mesa, ids_geo, noms_geo = item
                # Random pause to avoid blocking
                await asyncio.sleep(random.uniform(0.5, 1.5))
                await self.procesar_mesa_task(session, mesa, ids_geo, noms_geo)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"Worker Error: {e}")
            finally:
                queue.task_done()

    async def main(self):
        await self.init_db()
        queue = asyncio.Queue(maxsize=1000)

        # Limit HTTP/2 connections
        conn = aiohttp.TCPConnector(limit_per_host=4, ttl_dns_cache=300)

        timeout_config = aiohttp.ClientTimeout(total=30, connect=10)

        async with aiohttp.ClientSession(
            connector=conn, timeout=timeout_config
        ) as session:
            print(f"--- Starting Async Scraper ({self.NUM_WORKERS} workers) ---")
            workers = [
                asyncio.create_task(self.worker(queue, session))
                for _ in range(self.NUM_WORKERS)
            ]

            # List of Departments
            deptos_list = [
                {"id": "01", "nombre": "Atlantida"},
                {"id": "02", "nombre": "Colon"},
                {"id": "03", "nombre": "Comayagua"},
                {"id": "04", "nombre": "Copan"},
                {"id": "05", "nombre": "Cortes"},
                {"id": "06", "nombre": "Choluteca"},
                {"id": "07", "nombre": "El_Paraiso"},
                {"id": "08", "nombre": "Francisco_Morazan"},
                {"id": "09", "nombre": "Gracias_a_Dios"},
                {"id": "10", "nombre": "Intibuca"},
                {"id": "11", "nombre": "Islas_de_la_Bahia"},
                {"id": "12", "nombre": "La_Paz"},
                {"id": "13", "nombre": "Lempira"},
                {"id": "14", "nombre": "Ocotepeque"},
                {"id": "15", "nombre": "Olancho"},
                {"id": "16", "nombre": "Santa_Barbara"},
                {"id": "17", "nombre": "Valle"},
                {"id": "18", "nombre": "Yoro"},
                {"id": "20", "nombre": "Voto_Exterior"},
            ]

            for depto in deptos_list:
                url_munis = f"{self.BASE_API}/actas-documentos/{self.NIVEL}/{depto['id']}/municipios"
                munis = await self.fetch_navigation_robust(
                    session, url_munis, f"Municipalities of {depto['nombre']}"
                )
                print(f"Processing Department: {depto['nombre']}")

                for muni in munis:
                    id_muni = muni["id_municipio"]
                    nom_muni = muni["municipio"]

                    url_zonas = f"{self.BASE_API}/actas-documentos/{self.NIVEL}/{depto['id']}/{id_muni}/00/zonas"
                    zonas = await self.fetch_navigation_robust(
                        session, url_zonas, f"Zones of {nom_muni}"
                    )
                    print(
                        f"Processing Department: {depto['nombre']} Municipality: {muni['municipio']}"
                    )

                    MAPA_ZONAS_DEFAULT = {"01": "URBANA", "02": "RURAL"}

                    for zona in zonas:
                        id_zona = zona.get("cod_zona") or zona.get("id_zona")
                        nom_zona = zona.get("zona") or MAPA_ZONAS_DEFAULT.get(
                            id_zona, "Desconocida"
                        )

                        url_centros = f"{self.BASE_API}/actas-documentos/{self.NIVEL}/{depto['id']}/{id_muni}/{id_zona}/puestos"
                        centros = await self.fetch_navigation_robust(
                            session, url_centros, f"Centers in {nom_zona}"
                        )

                        for centro in centros:
                            id_centro = centro["id_puesto"]
                            nom_centro = centro["puesto"]

                            url_mesas = f"{self.BASE_API}/actas-documentos/{self.NIVEL}/{depto['id']}/{id_muni}/{id_zona}/{id_centro}/mesas"
                            mesas = await self.fetch_navigation_robust(
                                session, url_mesas, f"Polling Stations in {nom_centro}"
                            )

                            for mesa in mesas:
                                id_jrv = mesa.get("numero") or mesa.get("jrv")

                                if id_jrv and await self.verify_db_existance(
                                    int(id_jrv)
                                ):
                                    continue

                                ids_geo = {
                                    "depto": depto["id"],
                                    "muni": id_muni,
                                    "zona": id_zona,
                                    "centro": id_centro,
                                }
                                noms_geo = {
                                    "depto": depto["nombre"],
                                    "muni": nom_muni,
                                    "centro": nom_centro,
                                    "zona": nom_zona,
                                }

                                await queue.put((mesa, ids_geo, noms_geo))

            print("Hierarchy traversed. Processing job queue...")
            await queue.join()
            for w in workers:
                w.cancel()


if __name__ == "__main__":
    start_time = time.perf_counter()
    scraper = CNE_Scraper_Async()
    status_ejecucion = "SUCCESS"

    try:
        if os.name == "nt":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(scraper.main())
    except KeyboardInterrupt:
        status_ejecucion = "USER_INTERRUPTED"
        print("\n[!] Paused by user.")
    except Exception as e:
        status_ejecucion = f"ERROR: {str(e)}"
        print(f"\n[!] Critical Error: {e}")
    finally:
        end_time = time.perf_counter()
        total_seconds = end_time - start_time
        tiempo_formateado = f"{total_seconds:.2f}s"
        print("\n" + "=" * 50)
        print(f"⏱️  TIME: {tiempo_formateado}")
        print(f"📊 STATUS: {status_ejecucion}")
        print("=" * 50)

        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            log_tiempos = os.path.join(base_dir, "historial_tiempos.txt")
            with open(log_tiempos, "a", encoding="utf-8") as f:
                f.write(
                    f"[{datetime.now()}] | {tiempo_formateado} | {status_ejecucion}\n"
                )
        except:
            pass
