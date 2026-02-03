// Mapeo de comunidades autónomas españolas con sus códigos y coordenadas aproximadas
export const spanishRegions = {
  'Andalucía': { code: 'AN', color: '#3b82f6', lat: 37.5, lng: -4.5 },
  'Aragón': { code: 'AR', color: '#3b82f6', lat: 41.5, lng: -1.5 },
  'Asturias': { code: 'AS', color: '#3b82f6', lat: 43.3, lng: -5.5 },
  'Islas Baleares': { code: 'IB', color: '#3b82f6', lat: 39.5, lng: 3 },
  'Canarias': { code: 'CN', color: '#3b82f6', lat: 28, lng: -16 },
  'Cantabria': { code: 'CB', color: '#3b82f6', lat: 43.2, lng: -3.8 },
  'Castilla-La Mancha': { code: 'CM', color: '#3b82f6', lat: 39.5, lng: -2.5 },
  'Castilla y León': { code: 'CL', color: '#3b82f6', lat: 41.5, lng: -4 },
  'Cataluña': { code: 'CT', color: '#3b82f6', lat: 41.8, lng: 1.5 },
  'Comunidad de Madrid': { code: 'MD', color: '#3b82f6', lat: 40.4, lng: -3.7 },
  'Comunidad Foral de Navarra': { code: 'NC', color: '#3b82f6', lat: 42.5, lng: -1.5 },
  'Comunidad Valenciana': { code: 'VC', color: '#3b82f6', lat: 39.5, lng: -0.5 },
  'Extremadura': { code: 'EX', color: '#3b82f6', lat: 39, lng: -6 },
  'Galicia': { code: 'GA', color: '#3b82f6', lat: 42.5, lng: -8 },
  'La Rioja': { code: 'LR', color: '#3b82f6', lat: 42.3, lng: -2.5 },
  'País Vasco': { code: 'PV', color: '#3b82f6', lat: 43, lng: -2.5 },
  'Región de Murcia': { code: 'MC', color: '#3b82f6', lat: 37.8, lng: -1.3 },
  'Ceuta': { code: 'CE', color: '#3b82f6', lat: 35.9, lng: -5.3 },
  'Melilla': { code: 'ML', color: '#3b82f6', lat: 35.3, lng: -2.9 },
};

// Normalizar nombres de regiones
export function normalizeRegionName(name) {
  if (!name) return null;
  
  const normalized = name.trim().toLowerCase();
  
  // Búsqueda exacta normalizada
  for (const [region, data] of Object.entries(spanishRegions)) {
    if (region.toLowerCase() === normalized) {
      return region;
    }
  }
  
  // Búsqueda parcial
  for (const [region, data] of Object.entries(spanishRegions)) {
    if (region.toLowerCase().includes(normalized) || normalized.includes(region.toLowerCase())) {
      return region;
    }
  }
  
  return null;
}

// Obtener color según intensidad (heatmap)
export function getHeatmapColor(value, min, max) {
  if (max === 0) return '#e0f2fe';
  
  const normalized = (value - min) / (max - min);
  
  // Escala de colores: azul claro -> azul oscuro
  const colors = [
    '#e0f2fe', // Azul muy claro
    '#bae6fd', // Azul claro
    '#7dd3fc', // Azul medio-claro
    '#38bdf8', // Azul medio
    '#0ea5e9', // Azul
    '#0284c7', // Azul oscuro
    '#0369a1', // Azul más oscuro
    '#075985', // Azul muy oscuro
  ];
  
  const index = Math.floor(normalized * (colors.length - 1));
  return colors[Math.min(index, colors.length - 1)];
}

// Formatear moneda en euros
export function formatCurrency(value) {
  return new Intl.NumberFormat('es-ES', {
    style: 'currency',
    currency: 'EUR',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

// Formatear números grandes
export function formatNumber(value) {
  return new Intl.NumberFormat('es-ES').format(value);
}

// Obtener años disponibles en los datos
export function getAvailableYears(data) {
  const years = new Set();
  data.forEach(item => {
    if (item.anio) {
      years.add(item.anio);
    }
  });
  return Array.from(years).sort((a, b) => a - b);
}

// Agrupar datos por región y año
export function groupByRegionAndYear(data) {
  const grouped = {};
  
  data.forEach(item => {
    const region = normalizeRegionName(item.organo_nombre || item.beneficiario_nombre);
    if (!region) return;
    
    if (!grouped[region]) {
      grouped[region] = {};
    }
    
    const year = item.anio;
    if (!grouped[region][year]) {
      grouped[region][year] = {
        numero_concesiones: 0,
        importe_total: 0,
      };
    }
    
    grouped[region][year].numero_concesiones += item.numero_concesiones || 0;
    grouped[region][year].importe_total += item.importe_total || 0;
  });
  
  return grouped;
}

// Calcular totales por región (suma de todos los años)
export function calculateRegionTotals(groupedData) {
  const totals = {};
  
  for (const [region, years] of Object.entries(groupedData)) {
    totals[region] = {
      numero_concesiones: 0,
      importe_total: 0,
    };
    
    for (const yearData of Object.values(years)) {
      totals[region].numero_concesiones += yearData.numero_concesiones;
      totals[region].importe_total += yearData.importe_total;
    }
  }
  
  return totals;
}
<<<<<<< HEAD


=======
>>>>>>> e7805336fc8d7611291f74360e6db4a133e94ba0
