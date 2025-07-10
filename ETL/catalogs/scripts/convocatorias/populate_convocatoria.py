# load_convocatoria.py
# Pobla o actualiza una convocatoria a partir del JSON de detalle usando los modelos y relaciones definidos

from datetime import datetime
from app.db.session import get_session
from app.db.utils import normalizar, buscar_organo_id
from app.db.models import (
    Convocatoria, Documento, Anuncio, Finalidad, Fondo, Instrumento, Objetivo,
    Reglamento, Region, SectorActividad, SectorProducto, TipoBeneficiario
)

class IncompleteCatalog(Exception):
    def __init__(self, catalog, description):
        self.catalog = catalog
        self.description = description
        super().__init__(f"Catálogo incompleto: {catalog}: {description}")

def _parse_date(fecha):
    if not fecha:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(fecha, fmt).date()
        except Exception:
            continue
    return None

def get_or_create(session, Model, descripcion):
    norm = normalizar(descripcion)
    obj = session.query(Model).filter_by(descripcion_norm=norm).first()
    if not obj:
        raise IncompleteCatalog(Model.__tablename__, descripcion)
    return obj

def get_or_create_documento(session, item):
    doc = session.query(Documento).filter_by(id=item["id"]).first()
    if not doc:
        doc = Documento(
            id=item["id"],
            nombre_fic=item.get("nombreFic"),
            descripcion=item.get("descripcion"),
            longitud=item.get("long"),
            fecha_modificacion=_parse_date(item.get("datMod")),
            fecha_publicacion=_parse_date(item.get("datPublicacion")),
        )
        session.add(doc)
        session.flush()
    return doc

def get_or_create_anuncio(session, item):
    anuncio = session.query(Anuncio).filter_by(num_anuncio=item["numAnuncio"]).first()
    if not anuncio:
        anuncio = Anuncio(
            num_anuncio=item["numAnuncio"],
            titulo=item.get("titulo"),
            titulo_leng=item.get("tituloLeng"),
            texto=item.get("texto"),
            texto_leng=item.get("textoLeng"),
            url=item.get("url"),
            des_diario_oficial=item.get("desDiarioOficial"),
            fecha_publicacion=_parse_date(item.get("datPublicacion")),
        )
        session.add(anuncio)
        session.flush()
    return anuncio

def load_conocatorias_from_json(detalle):
    session = get_session()
    try:
        convocatoria_id = int(detalle["id"])
        codigo_bdns = str(detalle["codigoBDNS"])
        convocatoria = session.query(Convocatoria).filter_by(id=convocatoria_id).first()
        if not convocatoria:
            convocatoria = Convocatoria(id=convocatoria_id, codigo_bdns=codigo_bdns)
            session.add(convocatoria)

        # Poblar campos directos
        convocatoria.descripcion = detalle.get("descripcion")
        convocatoria.descripcion_leng = detalle.get("descripcionLeng")
        convocatoria.descripcion_finalidad = detalle.get("descripcionFinalidad")
        convocatoria.descripcion_bases = detalle.get("descripcionBasesReguladoras")
        convocatoria.url_bases = detalle.get("urlBasesReguladoras")
        convocatoria.url_ayuda_estado = detalle.get("urlAyudaEstado")
        convocatoria.ayuda_estado = detalle.get("ayudaEstado")
        convocatoria.tipo_convocatoria = detalle.get("tipoConvocatoria")
        convocatoria.sede_electronica = detalle.get("sedeElectronica")
        convocatoria.abierto = detalle.get("abierto")
        convocatoria.se_publica_diario_oficial = detalle.get("sePublicaDiarioOficial")
        convocatoria.presupuesto_total = detalle.get("presupuestoTotal")
        convocatoria.mrr = detalle.get("mrr")
        convocatoria.fecha_recepcion = _parse_date(detalle.get("fechaRecepcion"))
        convocatoria.fecha_inicio_solicitud = _parse_date(detalle.get("fechaInicioSolicitud"))
        convocatoria.fecha_fin_solicitud = _parse_date(detalle.get("fechaFinSolicitud"))

        # Organo (por niveles)
        organo = detalle.get("organo")
        if organo:
            nivel1 = organo.get("nivel1")
            nivel2 = organo.get("nivel2")
            nivel3 = organo.get("nivel3")
            organo_id = buscar_organo_id(session, nivel1, nivel2, nivel3)
            convocatoria.organo_id = organo_id

        # Reglamento y finalidad (1:1)
        if detalle.get("reglamento"):
            convocatoria.reglamento = get_or_create(session, Reglamento, detalle["reglamento"].get("descripcion", ""))

        if detalle.get("finalidad"):
            convocatoria.finalidad = get_or_create(session, Finalidad, detalle["finalidad"].get("descripcion", ""))

        # N:M relaciones
        def set_catalog_many(campo, Model, attr):
            items = detalle.get(campo) or []
            objs = []
            for item in items:
                if not item.get("descripcion"):
                    continue
                objs.append(get_or_create(session, Model, item["descripcion"]))
            setattr(convocatoria, attr, objs)

        set_catalog_many("instrumentos", Instrumento, "instrumentos")
        set_catalog_many("tiposBeneficiarios", TipoBeneficiario, "tipos_beneficiarios")
        set_catalog_many("sectores", SectorActividad, "sectores_actividad")
        set_catalog_many("sectoresProductos", SectorProducto, "sectores_producto")
        set_catalog_many("regiones", Region, "regiones")
        set_catalog_many("finalidades", Finalidad, "finalidades")
        set_catalog_many("objetivos", Objetivo, "objetivos")
        set_catalog_many("fondos", Fondo, "fondos")

        # Documentos y anuncios N:M
        convocatoria.documentos = []
        for item in detalle.get("documentos", []):
            doc = get_or_create_documento(session, item)
            convocatoria.documentos.append(doc)

        convocatoria.anuncios = []
        for item in detalle.get("anuncios", []):
            an = get_or_create_anuncio(session, item)
            convocatoria.anuncios.append(an)

        session.commit()
    finally:
        session.close()
