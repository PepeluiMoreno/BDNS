<template>
  <div class="bg-white rounded-lg shadow-md p-6">
    <div class="flex items-center justify-between mb-6">
      <h2 class="text-xl font-bold text-slate-900">Mapa de Calor Regional</h2>
      <div class="flex gap-2">
        <button
          @click="viewMode = 'concedente'"
          :class="[
            'px-4 py-2 rounded-lg font-medium text-sm transition-colors',
            viewMode === 'concedente'
              ? 'bg-primary-500 text-white'
              : 'bg-slate-100 text-slate-700 hover:bg-slate-200'
          ]"
        >
          Órgano Concedente
        </button>
        <button
          @click="viewMode = 'beneficiario'"
          :class="[
            'px-4 py-2 rounded-lg font-medium text-sm transition-colors',
            viewMode === 'beneficiario'
              ? 'bg-primary-500 text-white'
              : 'bg-slate-100 text-slate-700 hover:bg-slate-200'
          ]"
        >
          Beneficiario
        </button>
      </div>
    </div>

    <!-- Selector de año -->
    <div class="mb-6">
      <label class="block text-sm font-medium text-slate-700 mb-2">Año</label>
      <select
        v-model.number="selectedYear"
        class="w-full md:w-48 px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
      >
        <option v-for="year in availableYears" :key="year" :value="year">
          {{ year }}
        </option>
      </select>
    </div>

    <!-- Grid de regiones -->
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      <div
        v-for="(region, name) in regionData"
        :key="name"
        :style="{ borderLeftColor: region.color }"
        class="border-l-4 p-4 rounded-lg transition-all hover:shadow-md cursor-pointer"
        :class="{ 'bg-slate-50': !region.importe_total }"
        @click="selectRegion(name)"
      >
        <div class="flex items-start justify-between mb-2">
          <h3 class="font-semibold text-slate-900">{{ name }}</h3>
          <span v-if="region.importe_total" class="text-xs px-2 py-1 bg-primary-100 text-primary-700 rounded">
            {{ region.numero_concesiones }} concesiones
          </span>
        </div>
        <p v-if="region.importe_total" class="text-lg font-bold text-slate-900">
          {{ formatCurrency(region.importe_total) }}
        </p>
        <p v-else class="text-sm text-slate-500">Sin datos</p>
      </div>
    </div>

    <!-- Región seleccionada -->
    <div v-if="selectedRegionName" class="mt-8 pt-8 border-t border-slate-200">
      <h3 class="text-lg font-bold text-slate-900 mb-4">Detalles: {{ selectedRegionName }}</h3>
      <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div class="bg-primary-50 p-4 rounded-lg">
          <p class="text-sm text-slate-600 mb-1">Total de Concesiones</p>
          <p class="text-2xl font-bold text-primary-700">{{ selectedRegionData.numero_concesiones }}</p>
        </div>
        <div class="bg-primary-50 p-4 rounded-lg">
          <p class="text-sm text-slate-600 mb-1">Importe Total</p>
          <p class="text-2xl font-bold text-primary-700">{{ formatCurrency(selectedRegionData.importe_total) }}</p>
        </div>
        <div class="bg-primary-50 p-4 rounded-lg">
          <p class="text-sm text-slate-600 mb-1">Importe Medio</p>
          <p class="text-2xl font-bold text-primary-700">
            {{ formatCurrency(selectedRegionData.importe_total / (selectedRegionData.numero_concesiones || 1)) }}
          </p>
        </div>
      </div>
    </div>

    <!-- Leyenda -->
    <div class="mt-8 pt-8 border-t border-slate-200">
      <p class="text-sm font-medium text-slate-700 mb-3">Escala de Intensidad</p>
      <div class="flex items-center gap-2">
        <div class="w-6 h-6 bg-blue-100 rounded"></div>
        <span class="text-xs text-slate-600">Bajo</span>
        <div class="w-6 h-6 bg-blue-300 rounded"></div>
        <span class="text-xs text-slate-600">Medio</span>
        <div class="w-6 h-6 bg-blue-600 rounded"></div>
        <span class="text-xs text-slate-600">Alto</span>
        <div class="w-6 h-6 bg-blue-900 rounded"></div>
        <span class="text-xs text-slate-600">Muy Alto</span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { fetchEstadisticasPorOrgano, fetchEstadisticasPorTipoEntidad } from '../services/graphql';
import { spanishRegions, normalizeRegionName, getHeatmapColor, formatCurrency, getAvailableYears, groupByRegionAndYear, calculateRegionTotals } from '../utils/regions';

const viewMode = ref('concedente');
const selectedYear = ref(new Date().getFullYear());
const selectedRegionName = ref(null);
const allData = ref({
  concedente: [],
  beneficiario: [],
});

const availableYears = computed(() => {
  const data = viewMode.value === 'concedente' ? allData.value.concedente : allData.value.beneficiario;
  return getAvailableYears(data).reverse();
});

const regionData = computed(() => {
  const data = viewMode.value === 'concedente' ? allData.value.concedente : allData.value.beneficiario;
  const grouped = groupByRegionAndYear(data);
  const result = {};

  for (const [region, regionInfo] of Object.entries(spanishRegions)) {
    const yearData = grouped[region]?.[selectedYear.value];
    if (yearData) {
      result[region] = {
        ...yearData,
        color: getHeatmapColor(yearData.importe_total, 0, getMaxImporte(grouped)),
      };
    } else {
      result[region] = {
        numero_concesiones: 0,
        importe_total: 0,
        color: '#e0f2fe',
      };
    }
  }

  return result;
});

const selectedRegionData = computed(() => {
  if (!selectedRegionName.value) {
    return { numero_concesiones: 0, importe_total: 0 };
  }
  return regionData.value[selectedRegionName.value] || { numero_concesiones: 0, importe_total: 0 };
});

function getMaxImporte(grouped) {
  let max = 0;
  for (const yearData of Object.values(grouped)) {
    for (const data of Object.values(yearData)) {
      max = Math.max(max, data.importe_total);
    }
  }
  return max;
}

function selectRegion(name) {
  selectedRegionName.value = selectedRegionName.value === name ? null : name;
}

function formatCurrency(value) {
  return new Intl.NumberFormat('es-ES', {
    style: 'currency',
    currency: 'EUR',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

onMounted(async () => {
  const [concedente, beneficiario] = await Promise.all([
    fetchEstadisticasPorOrgano(),
    fetchEstadisticasPorTipoEntidad(),
  ]);
  
  allData.value.concedente = concedente;
  allData.value.beneficiario = beneficiario;
  
  if (availableYears.value.length > 0) {
    selectedYear.value = availableYears.value[0];
  }
});
</script>

<style scoped>
<<<<<<< HEAD
</style>
=======
</style>
>>>>>>> e7805336fc8d7611291f74360e6db4a133e94ba0
