<template>
  <div class="bg-white rounded-lg shadow-md p-6">
    <h2 class="text-xl font-bold text-slate-900 mb-6">Estadísticas por Comunidad Autónoma</h2>

    <!-- Controles -->
    <div class="mb-6 flex flex-col md:flex-row gap-4">
      <div class="flex-1">
        <input
          v-model="searchQuery"
          type="text"
          placeholder="Buscar comunidad autónoma..."
          class="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        />
      </div>
      <button
        @click="sortBy = sortBy === 'importe' ? 'concesiones' : 'importe'"
        class="px-4 py-2 bg-primary-500 text-white rounded-lg hover:bg-primary-600 transition-colors font-medium"
      >
        Ordenar por {{ sortBy === 'importe' ? 'Importe' : 'Concesiones' }}
      </button>
    </div>

    <!-- Tabla -->
    <div class="overflow-x-auto">
      <table class="w-full">
        <thead>
          <tr class="border-b-2 border-slate-200 bg-slate-50">
            <th class="px-4 py-3 text-left text-sm font-semibold text-slate-900">Comunidad Autónoma</th>
            <th v-for="year in years" :key="year" class="px-4 py-3 text-center text-sm font-semibold text-slate-900">
              {{ year }}
            </th>
            <th class="px-4 py-3 text-right text-sm font-semibold text-slate-900">Total</th>
            <th class="px-4 py-3 text-center text-sm font-semibold text-slate-900">Concesiones</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="(region, index) in filteredAndSortedData"
            :key="region.name"
            :class="[
              'border-b border-slate-200 hover:bg-slate-50 transition-colors',
              index % 2 === 0 ? 'bg-white' : 'bg-slate-50'
            ]"
          >
            <td class="px-4 py-3 font-medium text-slate-900">{{ region.name }}</td>
            <td v-for="year in years" :key="year" class="px-4 py-3 text-center text-sm text-slate-700">
              <span v-if="region.years[year]" class="text-primary-600 font-semibold">
                {{ formatCurrency(region.years[year].importe_total) }}
              </span>
              <span v-else class="text-slate-400">-</span>
            </td>
            <td class="px-4 py-3 text-right font-bold text-slate-900">
              {{ formatCurrency(region.total_importe) }}
            </td>
            <td class="px-4 py-3 text-center">
              <span class="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-primary-100 text-primary-700">
                {{ region.total_concesiones }}
              </span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Resumen -->
    <div class="mt-6 pt-6 border-t border-slate-200 grid grid-cols-1 md:grid-cols-3 gap-4">
      <div class="bg-primary-50 p-4 rounded-lg">
        <p class="text-sm text-slate-600 mb-1">Total de Regiones</p>
        <p class="text-2xl font-bold text-primary-700">{{ filteredAndSortedData.length }}</p>
      </div>
      <div class="bg-primary-50 p-4 rounded-lg">
        <p class="text-sm text-slate-600 mb-1">Total de Concesiones</p>
        <p class="text-2xl font-bold text-primary-700">{{ totalConcesiones }}</p>
      </div>
      <div class="bg-primary-50 p-4 rounded-lg">
        <p class="text-sm text-slate-600 mb-1">Importe Total</p>
        <p class="text-2xl font-bold text-primary-700">{{ formatCurrency(totalImporte) }}</p>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { fetchEstadisticasPorOrgano } from '../services/graphql';
import { spanishRegions, normalizeRegionName, getAvailableYears, groupByRegionAndYear } from '../utils/regions';

const searchQuery = ref('');
const sortBy = ref('importe');
const allData = ref([]);
const years = ref([]);

const groupedData = computed(() => {
  return groupByRegionAndYear(allData.value);
});

const tableData = computed(() => {
  const result = [];

  for (const [region] of Object.entries(spanishRegions)) {
    const regionYears = groupedData.value[region] || {};
    let total_importe = 0;
    let total_concesiones = 0;

    for (const yearData of Object.values(regionYears)) {
      total_importe += yearData.importe_total;
      total_concesiones += yearData.numero_concesiones;
    }

    result.push({
      name: region,
      years: regionYears,
      total_importe,
      total_concesiones,
    });
  }

  return result;
});

const filteredAndSortedData = computed(() => {
  let filtered = tableData.value;

  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase();
    filtered = filtered.filter(item => item.name.toLowerCase().includes(query));
  }

  return filtered.sort((a, b) => {
    if (sortBy.value === 'importe') {
      return b.total_importe - a.total_importe;
    } else {
      return b.total_concesiones - a.total_concesiones;
    }
  });
});

const totalConcesiones = computed(() => {
  return filteredAndSortedData.value.reduce((sum, item) => sum + item.total_concesiones, 0);
});

const totalImporte = computed(() => {
  return filteredAndSortedData.value.reduce((sum, item) => sum + item.total_importe, 0);
});

function formatCurrency(value) {
  if (!value) return '€0';
  return new Intl.NumberFormat('es-ES', {
    style: 'currency',
    currency: 'EUR',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

onMounted(async () => {
  const data = await fetchEstadisticasPorOrgano();
  allData.value = data;
  years.value = getAvailableYears(data).sort((a, b) => a - b);
});
</script>

<style scoped>
</style>

