import { GraphQLClient } from 'graphql-request';

// Configurar el cliente GraphQL
const API_URL = import.meta.env.VITE_GRAPHQL_URL || 'http://localhost:8000/graphql';

const client = new GraphQLClient(API_URL, {
  headers: {
    'Content-Type': 'application/json',
  },
});

// Queries para obtener datos de subvenciones
export const queries = {
  // Obtener estadísticas por región (concedente)
  estadisticasPorRegionConcedente: `
    query {
      estadisticas_por_organo {
        organo_nombre
        anio
        numero_concesiones
        importe_total
      }
    }
  `,

  // Obtener estadísticas por tipo de entidad (beneficiario)
  estadisticasPorTipoEntidad: `
    query {
      estadisticas_por_tipo_entidad {
        tipo_entidad
        anio
        numero_concesiones
        importe_total
      }
    }
  `,

  // Obtener concesiones con filtros
  concesiones: `
    query getConcesiones($filtros: ConcesionInput, $limite: Int, $offset: Int) {
      concesiones(filtros: $filtros, limite: $limite, offset: $offset) {
        id
        codigo_bdns
        convocatoria {
          id
          codigo_bdns
          titulo
          organo {
            id
            nombre
          }
        }
        beneficiario {
          id
          identificador
          nombre
          tipo
        }
        fecha_concesion
        importe
        descripcion_proyecto
        tipo_ayuda
        anio
      }
    }
  `,

  // Obtener beneficiarios
  beneficiarios: `
    query getBeneficiarios($filtros: BeneficiarioInput, $limite: Int, $offset: Int) {
      beneficiarios(filtros: $filtros, limite: $limite, offset: $offset) {
        id
        identificador
        nombre
        tipo
      }
    }
  `,

  // Obtener concentración de subvenciones
  concentracion: `
    query getConcentracion($anio: Int, $tipo_entidad: String, $limite: Int) {
      concentracion_subvenciones(anio: $anio, tipo_entidad: $tipo_entidad, limite: $limite) {
        beneficiario_nombre
        numero_concesiones
        importe_total
      }
    }
  `,
};

// Funciones para hacer queries
export async function fetchEstadisticasPorOrgano() {
  try {
    const data = await client.request(queries.estadisticasPorRegionConcedente);
    return data.estadisticas_por_organo || [];
  } catch (error) {
    console.error('Error fetching estadísticas por órgano:', error);
    return [];
  }
}

export async function fetchEstadisticasPorTipoEntidad() {
  try {
    const data = await client.request(queries.estadisticasPorTipoEntidad);
    return data.estadisticas_por_tipo_entidad || [];
  } catch (error) {
    console.error('Error fetching estadísticas por tipo entidad:', error);
    return [];
  }
}

export async function fetchConcesiones(filtros = {}, limite = 100, offset = 0) {
  try {
    const data = await client.request(queries.concesiones, {
      filtros: filtros || undefined,
      limite,
      offset,
    });
    return data.concesiones || [];
  } catch (error) {
    console.error('Error fetching concesiones:', error);
    return [];
  }
}

export async function fetchBeneficiarios(filtros = {}, limite = 100, offset = 0) {
  try {
    const data = await client.request(queries.beneficiarios, {
      filtros: filtros || undefined,
      limite,
      offset,
    });
    return data.beneficiarios || [];
  } catch (error) {
    console.error('Error fetching beneficiarios:', error);
    return [];
  }
}

export async function fetchConcentracion(anio = null, tipo_entidad = null, limite = 10) {
  try {
    const data = await client.request(queries.concentracion, {
      anio,
      tipo_entidad,
      limite,
    });
    return data.concentracion_subvenciones || [];
  } catch (error) {
    console.error('Error fetching concentración:', error);
    return [];
  }
}

export default client;
