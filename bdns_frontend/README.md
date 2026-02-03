# BDNS Dashboard Frontend

Dashboard profesional para visualizar y analizar datos de la Base de Datos Nacional de Subvenciones (BDNS) de España.

## Características

- **Dashboard Principal**: Visualización de estadísticas generales con contadores y totales
- **Mapa de Calor Interactivo**: Visualización regional de subvenciones por comunidad autónoma
  - Toggle entre vista de órgano concedente y beneficiario
  - Selector de año para análisis temporal
  - Escala de colores intuitiva
- **Tabla de Estadísticas**: Desglose por comunidad autónoma y año
  - Búsqueda y filtrado dinámico
  - Ordenamiento por importe o número de concesiones
- **Búsqueda Avanzada**: Panel completo de búsqueda de convocatorias
  - Filtros por año, beneficiario, órgano concedente, tipo de ayuda
  - Filtros de rango de importe
  - Paginación de resultados
- **Beneficiarios Internacionales**: Tabla separada para beneficiarios fuera de España
  - Información por país
  - Estadísticas de concesiones internacionales

## Tecnologías

- **Vue 3**: Framework frontend moderno
- **Vite**: Build tool y dev server rápido
- **Tailwind CSS**: Utilidades CSS para diseño responsive
- **GraphQL Request**: Cliente GraphQL para consumir la API
- **Chart.js**: Visualización de datos (preparado para futuras gráficas)

## Instalación

1. Instalar dependencias:
```bash
npm install
```

2. Configurar variables de entorno:
```bash
cp .env.example .env.local
# Editar .env.local con la URL del API GraphQL
```

## Desarrollo

Iniciar servidor de desarrollo en puerto 3000:
```bash
npm run dev
```

El servidor estará disponible en `http://localhost:3000`

## Build

Compilar para producción:
```bash
npm run build
```

Preview de la compilación:
```bash
npm run preview
```

## Estructura del Proyecto

```
src/
├── components/          # Componentes Vue reutilizables
│   ├── Header.vue      # Encabezado con información
│   ├── StatCard.vue    # Tarjetas de estadísticas
│   ├── RegionHeatmap.vue        # Mapa de calor regional
│   ├── RegionStatsTable.vue     # Tabla de estadísticas
│   ├── ConvocatoriaSearch.vue   # Panel de búsqueda
│   └── InternationalBeneficiaries.vue  # Tabla internacional
├── services/
│   └── graphql.js      # Cliente y queries GraphQL
├── utils/
│   └── regions.js      # Utilidades para regiones españolas
├── App.vue             # Componente raíz
├── main.js             # Punto de entrada
└── style.css           # Estilos globales con Tailwind
```

## Configuración de Colores

El tema utiliza una paleta de azul claro profesional:

- **Primary 50**: #f0f9ff (Azul muy claro)
- **Primary 500**: #0ea5e9 (Azul principal)
- **Primary 600**: #0284c7 (Azul oscuro)
- **Primary 700**: #0369a1 (Azul muy oscuro)

## Queries GraphQL Utilizadas

El frontend consume las siguientes queries:

- `estadisticas_por_organo`: Estadísticas agrupadas por órgano concedente
- `estadisticas_por_tipo_entidad`: Estadísticas por tipo de entidad beneficiaria
- `concesiones`: Búsqueda de concesiones con filtros
- `beneficiarios`: Búsqueda de beneficiarios
- `concentracion_subvenciones`: Análisis de concentración

## Rendimiento

- Lazy loading de componentes
- Caché en cliente de resultados GraphQL
- Paginación de resultados para tablas grandes
- Optimización de re-renders con computed properties

## Compatibilidad

- Chrome/Edge: ✅ Totalmente soportado
- Firefox: ✅ Totalmente soportado
- Safari: ✅ Totalmente soportado
- Mobile: ✅ Responsive design

## Próximas Mejoras

- [ ] Gráficas interactivas con Chart.js
- [ ] Exportación de datos a CSV/Excel
- [ ] Mapas geográficos más detallados
- [ ] Análisis de tendencias temporales
- [ ] Notificaciones en tiempo real
- [ ] Modo oscuro

## Licencia

MIT

## Contacto

Para más información sobre BDNS, visita: https://www.bdns.es/
