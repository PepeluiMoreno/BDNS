<script setup>
import { ref, watch } from 'vue';
import { fetchConcesiones, fetchConcesionesPorBeneficiario } from '../services/graphql';

const props = defineProps({
  concesion: {
    type: Object,
    default: null,
  },
});

const emit = defineEmits(['close', 'view-beneficiary-grants']);

const concesionesRelacionadas = ref([]);
const loadingRelated = ref(false);

const formatCurrency = (value) => {
  return new Intl.NumberFormat('es-ES', {
    style: 'currency',
    currency: 'EUR',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
};

const formatDate = (date) => {
  return new Date(date).toLocaleDateString('es-ES', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });
};

const loadRelatedGrants = async () => {
  if (!props.concesion?.beneficiario?.id) return;
  
  loadingRelated.value = true;
  try {
    const data = await fetchConcesionesPorBeneficiario(
      props.concesion.beneficiario.id,
      null,
      5
    );
    concesionesRelacionadas.value = data.filter(c => c.id !== props.concesion.id);
  } catch (error) {
    console.error('Error loading related grants:', error);
  } finally {
    loadingRelated.value = false;
  }
};

watch(
  () => props.concesion,
  (newVal) => {
    if (newVal) {
      loadRelatedGrants();
    }
  }
);

const viewBeneficiaryGrants = () => {
  emit('view-beneficiary-grants', props.concesion.beneficiario);
};
</script>

<template>
  <div v-if="concesion" class="fixed inset-0 bg-black bg-opacity-50 z-50 flex items-center justify-center p-4">
    <div class="bg-white rounded-lg shadow-xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
      <!-- Header -->
      <div class="sticky top-0 bg-gradient-to-r from-primary-50 to-primary-100 px-6 py-4 border-b border-primary-200 flex items-center justify-between">
        <h2 class="text-xl font-semibold text-slate-900">Detalle de Concesión</h2>
        <button
          @click="emit('close')"
          class="text-slate-500 hover:text-slate-700 transition-colors"
        >
          <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- Content -->
      <div class="p-6 space-y-6">
        <!-- Información de Convocatoria -->
        <div class="border border-slate-200 rounded-lg p-4 bg-slate-50">
          <h3 class="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-3">Convocatoria</h3>
          <div class="space-y-2">
            <div>
              <p class="text-xs text-slate-500 uppercase">Título</p>
              <p class="text-sm font-medium text-slate-900">{{ concesion.convocatoria?.titulo }}</p>
            </div>
            <div class="grid grid-cols-2 gap-4">
              <div>
                <p class="text-xs text-slate-500 uppercase">Código BDNS</p>
                <p class="text-sm font-medium text-slate-900">{{ concesion.convocatoria?.codigo_bdns }}</p>
              </div>
              <div>
                <p class="text-xs text-slate-500 uppercase">Órgano Concedente</p>
                <p class="text-sm font-medium text-slate-900">{{ concesion.convocatoria?.organo?.nombre }}</p>
              </div>
            </div>
          </div>
        </div>

        <!-- Información de Concesión -->
        <div class="border border-slate-200 rounded-lg p-4 bg-slate-50">
          <h3 class="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-3">Datos de la Concesión</h3>
          <div class="grid grid-cols-2 gap-4">
            <div>
              <p class="text-xs text-slate-500 uppercase">Código BDNS</p>
              <p class="text-sm font-medium text-slate-900">{{ concesion.codigo_bdns }}</p>
            </div>
            <div>
              <p class="text-xs text-slate-500 uppercase">Tipo de Ayuda</p>
              <p class="text-sm font-medium text-slate-900">{{ concesion.tipo_ayuda }}</p>
            </div>
            <div>
              <p class="text-xs text-slate-500 uppercase">Importe</p>
              <p class="text-lg font-semibold text-primary-600">{{ formatCurrency(concesion.importe) }}</p>
            </div>
            <div>
              <p class="text-xs text-slate-500 uppercase">Año</p>
              <p class="text-sm font-medium text-slate-900">{{ concesion.anio }}</p>
            </div>
            <div>
              <p class="text-xs text-slate-500 uppercase">Fecha de Concesión</p>
              <p class="text-sm font-medium text-slate-900">{{ formatDate(concesion.fecha_concesion) }}</p>
            </div>
            <div>
              <p class="text-xs text-slate-500 uppercase">Programa Presupuestario</p>
              <p class="text-sm font-medium text-slate-900">{{ concesion.programa_presupuestario || 'N/A' }}</p>
            </div>
          </div>
        </div>

        <!-- Información de Beneficiario -->
        <div class="border border-slate-200 rounded-lg p-4 bg-slate-50">
          <div class="flex items-center justify-between mb-3">
            <h3 class="text-sm font-semibold text-slate-700 uppercase tracking-wide">Beneficiario</h3>
            <button
              @click="viewBeneficiaryGrants"
              class="text-xs px-2 py-1 text-primary-600 hover:text-primary-700 hover:bg-primary-50 rounded transition-colors"
            >
              Ver todas las ayudas
            </button>
          </div>
          <div class="space-y-2">
            <div>
              <p class="text-xs text-slate-500 uppercase">Nombre</p>
              <p class="text-sm font-medium text-slate-900">{{ concesion.beneficiario?.nombre }}</p>
            </div>
            <div class="grid grid-cols-2 gap-4">
              <div>
                <p class="text-xs text-slate-500 uppercase">Identificador</p>
                <p class="text-sm font-medium text-slate-900">{{ concesion.beneficiario?.identificador }}</p>
              </div>
              <div>
                <p class="text-xs text-slate-500 uppercase">Tipo</p>
                <p class="text-sm font-medium text-slate-900">{{ concesion.beneficiario?.tipo }}</p>
              </div>
            </div>
          </div>
        </div>

        <!-- Descripción del Proyecto -->
        <div v-if="concesion.descripcion_proyecto" class="border border-slate-200 rounded-lg p-4 bg-slate-50">
          <h3 class="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-3">Descripción del Proyecto</h3>
          <p class="text-sm text-slate-700 leading-relaxed">{{ concesion.descripcion_proyecto }}</p>
        </div>

        <!-- Concesiones Relacionadas -->
        <div v-if="concesionesRelacionadas.length > 0" class="border border-slate-200 rounded-lg p-4 bg-slate-50">
          <h3 class="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-3">Otras Ayudas del Beneficiario</h3>
          <div class="space-y-2">
            <div
              v-for="grant in concesionesRelacionadas"
              :key="grant.id"
              class="flex items-center justify-between p-2 bg-white rounded border border-slate-200 hover:border-primary-300 transition-colors"
            >
              <div class="flex-1 min-w-0">
                <p class="text-sm font-medium text-slate-900 truncate">{{ grant.convocatoria?.titulo }}</p>
                <p class="text-xs text-slate-500">{{ grant.anio }}</p>
              </div>
              <p class="text-sm font-semibold text-primary-600 ml-2">{{ formatCurrency(grant.importe) }}</p>
            </div>
          </div>
        </div>
      </div>

      <!-- Footer -->
      <div class="sticky bottom-0 bg-slate-50 px-6 py-4 border-t border-slate-200 flex justify-end gap-3">
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