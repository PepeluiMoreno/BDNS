<template>
  <div class="bg-white rounded-lg shadow-md p-6">
    <h2 class="text-xl font-bold text-slate-900 mb-6">Beneficiarios Internacionales</h2>

    <!-- Información -->
    <div class="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
      <p class="text-sm text-blue-900">
        <strong>Nota:</strong> Esta tabla muestra los beneficiarios ubicados fuera del territorio español que han recibido subvenciones.
      </p>
    </div>

    <!-- Filtros -->
    <div class="mb-6 flex flex-col md:flex-row gap-4">
      <div class="flex-1">
        <input
          v-model="searchQuery"
          type="text"
          placeholder="Buscar por país o beneficiario..."
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
    <div v-if="filteredData.length > 0" class="overflow-x-auto">
      <table class="w-full">
        <thead>
          <tr class="border-b-2 border-slate-200 bg-slate-50">
            <th class="px-4 py-3 text-left text-sm font-semibold text-slate-900">País</th>
            <th class="px-4 py-3 text-left text-sm font-semibold text-slate-900">Beneficiario</th>
            <th class="px-4 py-3 text-center text-sm font-semibold text-slate-900">Concesiones</th>
            <th class="px-4 py-3 text-right text-sm font-semibold text-slate-900">Importe Total</th>
            <th class="px-4 py-3 text-center text-sm font-semibold text-slate-900">Importe Medio</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="(item, index) in paginatedData"
            :key="item.id"
            :class="[
              'border-b border-slate-200 hover:bg-slate-50 transition-colors',
              index % 2 === 0 ? 'bg-white' : 'bg-slate-50'
            ]"
          >
            <td class="px-4 py-3 font-medium text-slate-900">
              <span class="inline-flex items-center gap-2">
                <span class="text-lg">{{ getCountryFlag(item.pais) }}</span>
                {{ item.pais }}
              </span>
            </td>
            <td class="px-4 py-3 text-slate-700">
              <div class="font-medium">{{ item.nombre }}</div>
              <div class="text-xs text-slate-500">{{ item.identificador }}</div>
            </td>
            <td class="px-4 py-3 text-center">
              <span class="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-primary-100 text-primary-700">
                {{ item.numero_concesiones }}
              </span>
            </td>
            <td class="px-4 py-3 text-right font-bold text-slate-900">
              {{ formatCurrency(item.importe_total) }}
            </td>
            <td class="px-4 py-3 text-center text-slate-700">
              {{ formatCurrency(item.importe_total / item.numero_concesiones) }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Sin datos -->
    <div v-else class="text-center py-8">
      <svg class="w-12 h-12 text-slate-400 mx-auto mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
      <p class="text-slate-600">No hay beneficiarios internacionales con los filtros seleccionados</p>
    </div>

    <!-- Paginación -->
    <div v-if="totalPages > 1" class="mt-6 pt-6 border-t border-slate-200 flex items-center justify-between">
      <p class="text-sm text-slate-600">
        Página {{ currentPage }} de {{ totalPages }} ({{ filteredData.length }} resultados)
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

    <!-- Resumen -->
    <div v-if="filteredData.length > 0" class="mt-6 pt-6 border-t border-slate-200 grid grid-cols-1 md:grid-cols-3 gap-4">
      <div class="bg-primary-50 p-4 rounded-lg">
        <p class="text-sm text-slate-600 mb-1">Total de Países</p>
        <p class="text-2xl font-bold text-primary-700">{{ uniqueCountries }}</p>
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
import { fetchBeneficiarios } from '../services/graphql';

const searchQuery = ref('');
const sortBy = ref('importe');
const currentPage = ref(1);
const itemsPerPage = 10;
const allData = ref([]);

// Mapeo de países a banderas (emoji)
const countryFlags = {
  'Francia': '🇫🇷',
  'Alemania': '🇩🇪',
  'Italia': '🇮🇹',
  'Portugal': '🇵🇹',
  'Reino Unido': '🇬🇧',
  'Bélgica': '🇧🇪',
  'Países Bajos': '🇳🇱',
  'Suiza': '🇨🇭',
  'Austria': '🇦🇹',
  'Suecia': '🇸🇪',
  'Noruega': '🇳🇴',
  'Dinamarca': '🇩🇰',
  'Finlandia': '🇫🇮',
  'Irlanda': '🇮🇪',
  'Grecia': '🇬🇷',
  'Luxemburgo': '🇱🇺',
  'Eslovenia': '🇸🇮',
  'Eslovaquia': '🇸🇰',
  'República Checa': '🇨🇿',
  'Hungría': '🇭🇺',
  'Polonia': '🇵🇱',
  'Rumania': '🇷🇴',
  'Bulgaria': '🇧🇬',
  'Croacia': '🇭🇷',
  'Chipre': '🇨🇾',
  'Malta': '🇲🇹',
  'Lituania': '🇱🇹',
  'Letonia': '🇱🇻',
  'Estonia': '🇪🇪',
  'Estados Unidos': '🇺🇸',
  'Canadá': '🇨🇦',
  'México': '🇲🇽',
  'Brasil': '🇧🇷',
  'Argentina': '🇦🇷',
  'Chile': '🇨🇱',
  'China': '🇨🇳',
  'Japón': '🇯🇵',
  'India': '🇮🇳',
  'Australia': '🇦🇺',
  'Nueva Zelanda': '🇳🇿',
};

const filteredData = computed(() => {
  let filtered = allData.value;

  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase();
    filtered = filtered.filter(item =>
      item.pais?.toLowerCase().includes(query) ||
      item.nombre?.toLowerCase().includes(query) ||
      item.identificador?.toLowerCase().includes(query)
    );
  }

  return filtered.sort((a, b) => {
    if (sortBy.value === 'importe') {
      return b.importe_total - a.importe_total;
    } else {
      return b.numero_concesiones - a.numero_concesiones;
    }
  });
});

const totalPages = computed(() => {
  return Math.ceil(filteredData.value.length / itemsPerPage);
});

const paginatedData = computed(() => {
  const start = (currentPage.value - 1) * itemsPerPage;
  const end = start + itemsPerPage;
  return filteredData.value.slice(start, end);
});

const uniqueCountries = computed(() => {
  return new Set(filteredData.value.map(item => item.pais)).size;
});

const totalConcesiones = computed(() => {
  return filteredData.value.reduce((sum, item) => sum + item.numero_concesiones, 0);
});

const totalImporte = computed(() => {
  return filteredData.value.reduce((sum, item) => sum + item.importe_total, 0);
});

function getCountryFlag(country) {
  return countryFlags[country] || '🌍';
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
  // Simulamos datos de beneficiarios internacionales
  // En producción, esto vendría del API
  const beneficiarios = await fetchBeneficiarios({}, 1000, 0);
  
  // Agrupar por país (simulado)
  const grouped = {};
  beneficiarios.forEach(item => {
    const pais = item.tipo === 'Extranjero' ? 'Francia' : null; // Simplificado
    if (pais) {
      if (!grouped[pais]) {
        grouped[pais] = {
          pais,
          nombre: item.nombre,
          identificador: item.identificador,
          numero_concesiones: 0,
          importe_total: 0,
          id: item.id,
        };
      }
      grouped[pais].numero_concesiones += 1;
    }
  });

  allData.value = Object.values(grouped);
});
</script>

<style scoped>
<<<<<<< HEAD
</style>
=======
</style>
>>>>>>> e7805336fc8d7611291f74360e6db4a133e94ba0
