/* ConcesionesPorBeneficiario.vue */

<script setup>
import { ref, computed, watch } from 'vue';
import { fetchConcesionesPorBeneficiario } from '../services/graphql';

const props = defineProps({
  beneficiario: {
    type: Object,
    default: null,
  },
});

const emit = defineEmits(['close']);

const concesiones = ref([]);
const loading = ref(false);
const currentPage = ref(1);
const itemsPerPage = ref(10);
const selectedYear = ref(null);

const paginatedConcesiones = computed(() => {
  let filtered = concesiones.value;
  if (selectedYear.value) {
    filtered = filtered.filter(c => c.anio === selectedYear.value);
  }
  const start = (currentPage.value - 1) * itemsPerPage.value;
  const end = start + itemsPerPage.value;
  return filtered.slice(start, end);
});

const totalItems = computed(() => {
  if (selectedYear.value) {
    return concesiones.value.filter(c => c.anio === selectedYear.value).length;
  }
  return concesiones.value.length;
});

const totalPages = computed(() => {
  return Math.ceil(totalItems.value / itemsPerPage.value);
});

const years = computed(() => {
  const uniqueYears = [...new Set(concesiones.value.map(c => c.anio))];
  return uniqueYears.sort((a, b) => b - a);
});

const totalAmount = computed(() => {
  let filtered = concesiones.value;
  if (selectedYear.value) {
    filtered = filtered.filter(c => c.anio === selectedYear.value);
  }
  return filtered.reduce((sum, c) => sum + (c.importe || 0), 0);
});

const loadConcesiones = async () => {
  if (!props.beneficiario?.id) return;
  
  loading.value = true;
  try {
    const data = await fetchConcesionesPorBeneficiario(
      props.beneficiario.id,
      null,
      1000
    );
    concesiones.value = data;
  } catch (error) {
    console.error('Error loading concesiones:', error);
  } finally {
    loading.value = false;
  }
};

const formatCurrency = (value) => {
  return new Intl.NumberFormat('es-ES', {
    style: 'currency',
    currency: 'EUR',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
};

const formatDate = (date) => {
  return new Date(date).toLocaleDateString('es-ES');
};

const goToPage = (page) => {
  if (page >= 1 && page <= totalPages.value) {
    currentPage.value = page;
  }
};

watch(
  () => props.beneficiario,
  (newVal) => {
    if (newVal) {
      currentPage.value = 1;
      selectedYear.value = null;
      loadConcesiones();
    }
  }
);

watch(selectedYear, () => {
  currentPage.value = 1;
});
</script>

<template>
  <div v-if="beneficiario" class="fixed inset-0 bg-black bg-opacity-50 z-50 flex items-center justify-center p-4">
    <div class="bg-white rounded-lg shadow-xl max-w-4xl w-full max-h-[90vh] overflow-y-auto">
      <!-- Header -->
      <div class="sticky top-0 bg-gradient-to-r from-primary-50 to-primary-100 px-6 py-4 border-b border-primary-200">
        <div class="flex items-center justify-between">
          <div>
            <h2 class="text-xl font-semibold text-slate-900">Ayudas del Beneficiario</h2>
            <p class="text-sm text-slate-600 mt-1">{{ beneficiario.nombre }}</p>
          </div>
          <button
            @click="emit('close')"
            class="text-slate-500 hover:text-slate-700 transition-colors"
          >
            <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      </div>

      <!-- Content -->
      <div class="p-6 space-y-6">
        <!-- Información del Beneficiario -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div class="bg-primary-50 rounded-lg p-4 border border-primary-200">
            <p class="text-xs text-primary-700 uppercase font-semibold">Total de Ayudas</p>
            <p class="text-2xl font-bold text-primary-600 mt-1">{{ concesiones.length }}</p>
          </div>
          <div class="bg-green-50 rounded-lg p-4 border border-green-200">
            <p class="text-xs text-green-700 uppercase font-semibold">Importe Total</p>
            <p class="text-2xl font-bold text-green-600 mt-1">{{ formatCurrency(concesiones.reduce((sum, c) => sum + (c.importe || 0), 0)) }}</p>
          </div>
          <div class="bg-blue-50 rounded-lg p-4 border border-blue-200">
            <p class="text-xs text-blue-700 uppercase font-semibold">Años Cubiertos</p>
            <p class="text-2xl font-bold text-blue-600 mt-1">{{ years.length }}</p>
          </div>
        </div>

        <!-- Filtro por Año -->
        <div class="flex items-center gap-4">
          <label class="text-sm font-medium text-slate-700">Filtrar por año:</label>
          <select
            v-model.number="selectedYear"
            class="px-3 py-2 border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500"
          >
            <option :value="null">Todos los años</option>
            <option v-for="year in years" :key="year" :value="year">{{ year }}</option>
          </select>
          <div class="ml-auto text-sm text-slate-600">
            Mostrando <span class="font-semibold">{{ totalItems }}</span> ayudas
            <span v-if="selectedYear">({{ selectedYear }})</span>
          </div>
        </div>

        <!-- Loading State -->
        <div v-if="loading" class="p-8 text-center">
          <div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
          <p class="mt-2 text-slate-600">Cargando ayudas...</p>
        </div>

        <!-- Table -->
        <div v-else class="overflow-x-auto">
          <table class="w-full">
            <thead>
              <tr class="bg-slate-50 border-b border-slate-200">
                <th class="px-6 py-3 text-left text-xs font-semibold text-slate-700 uppercase tracking-wider">Convocatoria</th>
                <th class="px-6 py-3 text-left text-xs font-semibold text-slate-700 uppercase tracking-wider">Órgano</th>
                <th class="px-6 py-3 text-right text-xs font-semibold text-slate-700 uppercase tracking-wider">Importe</th>
                <th class="px-6 py-3 text-center text-xs font-semibold text-slate-700 uppercase tracking-wider">Año</th>
                <th class="px-6 py-3 text-center text-xs font-semibold text-slate-700 uppercase tracking-wider">Fecha</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="concesion in paginatedConcesiones"
                :key="concesion.id"
                class="border-b border-slate-200 hover:bg-slate-50 transition-colors"
              >
                <td class="px-6 py-4">
                  <div class="text-sm font-medium text-slate-900 truncate max-w-xs">
                    {{ concesion.convocatoria?.titulo }}
                  </div>
                  <div class="text-xs text-slate-500">{{ concesion.codigo_bdns }}</div>
                </td>
                <td class="px-6 py-4">
                  <div class="text-sm text-slate-700">{{ concesion.convocatoria?.organo?.nombre }}</div>
                </td>
                <td class="px-6 py-4 text-right">
                  <span class="text-sm font-semibold text-primary-600">{{ formatCurrency(concesion.importe) }}</span>
                </td>
                <td class="px-6 py-4 text-center">
                  <span class="text-sm text-slate-600">{{ concesion.anio }}</span>
                </td>
                <td class="px-6 py-4 text-center">
                  <span class="text-sm text-slate-600">{{ formatDate(concesion.fecha_concesion) }}</span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Pagination -->
        <div v-if="totalPages > 1" class="flex items-center justify-between">
          <div class="text-sm text-slate-600">
            Página <span class="font-semibold">{{ currentPage }}</span> de <span class="font-semibold">{{ totalPages }}</span>
          </div>
          <div class="flex gap-2">
            <button
              @click="goToPage(currentPage - 1)"
              :disabled="currentPage === 1"
              class="px-3 py-1 text-sm border border-slate-300 rounded hover:bg-slate-100 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              Anterior
            </button>
            <button
              v-for="page in Math.min(5, totalPages)"
              :key="page"
              @click="goToPage(page)"
              :class="[
                'px-3 py-1 text-sm rounded transition-colors',
                page === currentPage
                  ? 'bg-primary-600 text-white'
                  : 'border border-slate-300 hover:bg-slate-100'
              ]"
            >
              {{ page }}
            </button>
            <button
              @click="goToPage(currentPage + 1)"
              :disabled="currentPage === totalPages"
              class="px-3 py-1 text-sm border border-slate-300 rounded hover:bg-slate-100 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              Siguiente
            </button>
          </div>
        </div>
      </div>

      <!-- Footer -->
      <div class="sticky bottom-0 bg-slate-50 px-6 py-4 border-t border-slate-200 flex justify-end">
        <button
          @click="emit('close')"
          class="px-4 py-2 text-sm font-medium text-slate-700 bg-white border border-slate-300 rounded-lg hover:bg-slate-50 transition-colors"
        >
          Cerrar
        </button>
      </div>
    </div>
  </div>
</template>


