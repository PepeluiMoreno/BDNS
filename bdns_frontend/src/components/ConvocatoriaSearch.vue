<template>
  <div class="bg-white rounded-lg shadow-md p-6">
    <h2 class="text-xl font-bold text-slate-900 mb-6">Búsqueda Avanzada de Convocatorias</h2>

    <!-- Filtros -->
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
      <!-- Año de Publicación -->
      <div>
        <label class="block text-sm font-medium text-slate-700 mb-2">Año de Publicación</label>
        <select
          v-model.number="filters.year"
          class="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        >
          <option :value="null">Todos los años</option>
          <option v-for="year in availableYears" :key="year" :value="year">{{ year }}</option>
        </select>
      </div>

      <!-- Beneficiario -->
      <div>
        <label class="block text-sm font-medium text-slate-700 mb-2">Beneficiario</label>
        <input
          v-model="filters.beneficiary"
          type="text"
          placeholder="Nombre o NIF del beneficiario"
          class="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        />
      </div>

      <!-- Órgano Concedente -->
      <div>
        <label class="block text-sm font-medium text-slate-700 mb-2">Órgano Concedente</label>
        <input
          v-model="filters.organo"
          type="text"
          placeholder="Nombre del órgano"
          class="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        />
      </div>

      <!-- Tipo de Ayuda -->
      <div>
        <label class="block text-sm font-medium text-slate-700 mb-2">Tipo de Ayuda</label>
        <select
          v-model="filters.tipoAyuda"
          class="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        >
          <option value="">Todos</option>
          <option value="Subvención">Subvención</option>
          <option value="Crédito">Crédito</option>
          <option value="Garantía">Garantía</option>
          <option value="Otra">Otra</option>
        </select>
      </div>

      <!-- Importe Mínimo -->
      <div>
        <label class="block text-sm font-medium text-slate-700 mb-2">Importe Mínimo (€)</label>
        <input
          v-model.number="filters.importeMin"
          type="number"
          placeholder="0"
          class="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        />
      </div>

      <!-- Importe Máximo -->
      <div>
        <label class="block text-sm font-medium text-slate-700 mb-2">Importe Máximo (€)</label>
        <input
          v-model.number="filters.importeMax"
          type="number"
          placeholder="Sin límite"
          class="w-full px-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        />
      </div>
    </div>

    <!-- Botones de acción -->
    <div class="flex gap-3 mb-6">
      <button
        @click="search"
        class="px-6 py-2 bg-primary-500 text-white rounded-lg hover:bg-primary-600 transition-colors font-medium flex items-center gap-2"
      >
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
        </svg>
        Buscar
      </button>
      <button
        @click="resetFilters"
        class="px-6 py-2 bg-slate-200 text-slate-700 rounded-lg hover:bg-slate-300 transition-colors font-medium"
      >
        Limpiar Filtros
      </button>
    </div>

    <!-- Resultados -->
    <div v-if="loading" class="text-center py-8">
      <div class="inline-block animate-spin">
        <svg class="w-8 h-8 text-primary-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
        </svg>
      </div>
      <p class="text-slate-600 mt-2">Cargando resultados...</p>
    </div>

    <div v-else-if="results.length > 0">
      <div class="mb-4 flex items-center justify-between">
        <p class="text-sm text-slate-600">
          Se encontraron <span class="font-bold text-primary-600">{{ results.length }}</span> resultados
        </p>
        <select
          v-model.number="itemsPerPage"
          class="px-3 py-1 border border-slate-300 rounded text-sm focus:ring-2 focus:ring-primary-500 focus:border-transparent"
        >
          <option :value="10">10 por página</option>
          <option :value="25">25 por página</option>
          <option :value="50">50 por página</option>
        </select>
      </div>

      <div class="overflow-x-auto">
        <table class="w-full text-sm">
          <thead>
            <tr class="border-b-2 border-slate-200 bg-slate-50">
              <th class="px-4 py-3 text-left font-semibold text-slate-900">Beneficiario</th>
              <th class="px-4 py-3 text-left font-semibold text-slate-900">Órgano Concedente</th>
              <th class="px-4 py-3 text-center font-semibold text-slate-900">Importe</th>
              <th class="px-4 py-3 text-center font-semibold text-slate-900">Fecha</th>
              <th class="px-4 py-3 text-center font-semibold text-slate-900">Tipo</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="(result, index) in paginatedResults"
              :key="result.id"
              :class="[
                'border-b border-slate-200 hover:bg-slate-50 transition-colors',
                index % 2 === 0 ? 'bg-white' : 'bg-slate-50'
              ]"
            >
              <td class="px-4 py-3 font-medium text-slate-900">
                <div>{{ result.beneficiario?.nombre }}</div>
                <div class="text-xs text-slate-500">{{ result.beneficiario?.identificador }}</div>
              </td>
              <td class="px-4 py-3 text-slate-700">{{ result.convocatoria?.organo?.nombre }}</td>
              <td class="px-4 py-3 text-center font-semibold text-primary-600">
                {{ formatCurrency(result.importe) }}
              </td>
              <td class="px-4 py-3 text-center text-slate-700">
                {{ formatDate(result.fecha_concesion) }}
              </td>
              <td class="px-4 py-3 text-center">
                <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                  {{ result.tipo_ayuda }}
                </span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Paginación -->
      <div v-if="totalPages > 1" class="mt-6 flex items-center justify-between">
        <p class="text-sm text-slate-600">
          Página {{ currentPage }} de {{ totalPages }}
        </p>
        <div class="flex gap-2">
          <button
            @click="currentPage = Math.max(1, currentPage - 1)"
            :disabled="currentPage === 1"
            class="px-3 py-1 border border-slate-300 rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-slate-50"
          >
            ← Anterior
          </button>
          <button
            @click="currentPage = Math.min(totalPages, currentPage + 1)"
            :disabled="currentPage === totalPages"
            class="px-3 py-1 border border-slate-300 rounded-lg disabled:opacity-50 disabled:cursor-not-allowed hover:bg-slate-50"
          >
            Siguiente →
          </button>
        </div>
      </div>
    </div>

    <div v-else-if="searched && !loading" class="text-center py-8">
      <svg class="w-12 h-12 text-slate-400 mx-auto mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
      <p class="text-slate-600">No se encontraron resultados con los filtros seleccionados</p>
    </div>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue';
import { fetchConcesiones } from '../services/graphql';

const filters = ref({
  year: null,
  beneficiary: '',
  organo: '',
  tipoAyuda: '',
  importeMin: null,
  importeMax: null,
});

const results = ref([]);
const loading = ref(false);
const searched = ref(false);
const currentPage = ref(1);
const itemsPerPage = ref(10);
const availableYears = ref([2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]);

const totalPages = computed(() => {
  return Math.ceil(results.value.length / itemsPerPage.value);
});

const paginatedResults = computed(() => {
  const start = (currentPage.value - 1) * itemsPerPage.value;
  const end = start + itemsPerPage.value;
  return results.value.slice(start, end);
});

async function search() {
  loading.value = true;
  searched.value = true;
  currentPage.value = 1;

  try {
    const filtrosGraphQL = {};
    
    if (filters.value.year) {
      filtrosGraphQL.anio = filters.value.year;
    }
    if (filters.value.importeMin !== null) {
      filtrosGraphQL.importe_minimo = filters.value.importeMin;
    }
    if (filters.value.importeMax !== null) {
      filtrosGraphQL.importe_maximo = filters.value.importeMax;
    }
    if (filters.value.tipoAyuda) {
      filtrosGraphQL.tipo_ayuda = filters.value.tipoAyuda;
    }

    const data = await fetchConcesiones(filtrosGraphQL, 1000, 0);
    
    // Filtrado adicional en cliente
    results.value = data.filter(item => {
      if (filters.value.beneficiary && !item.beneficiario?.nombre?.toLowerCase().includes(filters.value.beneficiary.toLowerCase())) {
        return false;
      }
      if (filters.value.organo && !item.convocatoria?.organo?.nombre?.toLowerCase().includes(filters.value.organo.toLowerCase())) {
        return false;
      }
      return true;
    });
  } catch (error) {
    console.error('Error searching concesiones:', error);
    results.value = [];
  } finally {
    loading.value = false;
  }
}

function resetFilters() {
  filters.value = {
    year: null,
    beneficiary: '',
    organo: '',
    tipoAyuda: '',
    importeMin: null,
    importeMax: null,
  };
  results.value = [];
  searched.value = false;
  currentPage.value = 1;
}

function formatCurrency(value) {
  return new Intl.NumberFormat('es-ES', {
    style: 'currency',
    currency: 'EUR',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

function formatDate(dateString) {
  if (!dateString) return '-';
  const date = new Date(dateString);
  return date.toLocaleDateString('es-ES');
}
</script>

<style scoped>
<<<<<<< HEAD
</style>
=======
</style>
>>>>>>> e7805336fc8d7611291f74360e6db4a133e94ba0
