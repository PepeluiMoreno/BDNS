/* ConvocatoriaList.vue */
<script setup>
import { ref, computed, onMounted } from 'vue';
import { fetchConcesiones } from '../services/graphql';

const emit = defineEmits(['select-concesion', 'select-convocatoria']);

const props = defineProps({
  filters: {
    type: Object,
    default: () => ({}),
  },
});

const concesiones = ref([]);
const loading = ref(false);
const currentPage = ref(1);
const itemsPerPage = ref(10);
const totalItems = ref(0);
const selectedConcesion = ref(null);

const paginatedConcesiones = computed(() => {
  const start = (currentPage.value - 1) * itemsPerPage.value;
  const end = start + itemsPerPage.value;
  return concesiones.value.slice(start, end);
});

const totalPages = computed(() => {
  return Math.ceil(totalItems.value / itemsPerPage.value);
});

const loadConcesiones = async () => {
  loading.value = true;
  try {
    const offset = (currentPage.value - 1) * itemsPerPage.value;
    const data = await fetchConcesiones(props.filters, itemsPerPage.value, offset);
    concesiones.value = data;
    totalItems.value = data.length > 0 ? data.length + offset : offset;
  } catch (error) {
    console.error('Error loading concesiones:', error);
  } finally {
    loading.value = false;
  }
};

const selectConcesion = (concesion) => {
  selectedConcesion.value = concesion.id;
  emit('select-concesion', concesion);
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

onMounted(loadConcesiones);

const goToPage = (page) => {
  if (page >= 1 && page <= totalPages.value) {
    currentPage.value = page;
    loadConcesiones();
  }
};

watch(
  () => props.filters,
  () => {
    currentPage.value = 1;
    loadConcesiones();
  },
  { deep: true }
);
</script>

<template>
  <div class="bg-white rounded-lg shadow-md overflow-hidden">
    <!-- Header -->
    <div class="bg-gradient-to-r from-primary-50 to-primary-100 px-6 py-4 border-b border-primary-200">
      <h3 class="text-lg font-semibold text-slate-900 flex items-center gap-2">
        <svg class="w-5 h-5 text-primary-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
        Listado de Concesiones
        <span class="text-sm font-normal text-slate-600 ml-auto">{{ totalItems }} resultados</span>
      </h3>
    </div>

    <!-- Loading State -->
    <div v-if="loading" class="p-8 text-center">
      <div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
      <p class="mt-2 text-slate-600">Cargando concesiones...</p>
    </div>

    <!-- Empty State -->
    <div v-else-if="concesiones.length === 0" class="p-8 text-center">
      <svg class="w-12 h-12 text-slate-400 mx-auto mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
      </svg>
      <p class="text-slate-600">No se encontraron concesiones con los filtros aplicados</p>
    </div>

    <!-- Table -->
    <div v-else class="overflow-x-auto">
      <table class="w-full">
        <thead>
          <tr class="bg-slate-50 border-b border-slate-200">
            <th class="px-6 py-3 text-left text-xs font-semibold text-slate-700 uppercase tracking-wider">Convocatoria</th>
            <th class="px-6 py-3 text-left text-xs font-semibold text-slate-700 uppercase tracking-wider">Órgano</th>
            <th class="px-6 py-3 text-left text-xs font-semibold text-slate-700 uppercase tracking-wider">Beneficiario</th>
            <th class="px-6 py-3 text-right text-xs font-semibold text-slate-700 uppercase tracking-wider">Importe</th>
            <th class="px-6 py-3 text-center text-xs font-semibold text-slate-700 uppercase tracking-wider">Fecha</th>
            <th class="px-6 py-3 text-center text-xs font-semibold text-slate-700 uppercase tracking-wider">Acción</th>
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
                {{ concesion.convocatoria?.titulo || 'N/A' }}
              </div>
              <div class="text-xs text-slate-500">{{ concesion.codigo_bdns }}</div>
            </td>
            <td class="px-6 py-4">
              <div class="text-sm text-slate-700">{{ concesion.convocatoria?.organo?.nombre || 'N/A' }}</div>
            </td>
            <td class="px-6 py-4">
              <div class="text-sm text-slate-700 truncate max-w-xs">{{ concesion.beneficiario?.nombre || 'N/A' }}</div>
              <div class="text-xs text-slate-500">{{ concesion.beneficiario?.tipo }}</div>
            </td>
            <td class="px-6 py-4 text-right">
              <span class="text-sm font-semibold text-primary-600">{{ formatCurrency(concesion.importe) }}</span>
            </td>
            <td class="px-6 py-4 text-center">
              <span class="text-sm text-slate-600">{{ formatDate(concesion.fecha_concesion) }}</span>
            </td>
            <td class="px-6 py-4 text-center">
              <button
                @click="selectConcesion(concesion)"
                class="inline-flex items-center gap-1 px-3 py-1 text-sm text-primary-600 hover:text-primary-700 hover:bg-primary-50 rounded transition-colors"
              >
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                </svg>
                Ver
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Pagination -->
    <div v-if="totalPages > 1" class="bg-slate-50 px-6 py-4 border-t border-slate-200 flex items-center justify-between">
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
</template>

<script>
import { watch } from 'vue';
</script>