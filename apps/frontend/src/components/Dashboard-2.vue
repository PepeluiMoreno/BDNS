
/* Dashboard-2.vue */

<script setup>
import { ref } from 'vue';
import ConvocatoriaFilter from './ConvocatoriaFilter.vue';
import ConvocatoriaList from './ConvocatoriaList.vue';
import ConvocatoriaDetail from './ConvocatoriaDetail.vue';
import BeneficiaryGrants from './BeneficiaryGrants.vue';

const filters = ref({});
const selectedConcesion = ref(null);
const selectedBeneficiario = ref(null);

const handleFilter = (newFilters) => {
  filters.value = newFilters;
};

const handleSelectConcesion = (concesion) => {
  selectedConcesion.value = concesion;
};

const handleCloseConcesionDetail = () => {
  selectedConcesion.value = null;
};

const handleViewBeneficiaryGrants = (beneficiario) => {
  selectedBeneficiario.value = beneficiario;
};

const handleCloseBeneficiaryGrants = () => {
  selectedBeneficiario.value = null;
};
</script>

<template>
  <div class="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100">
    <!-- Header -->
    <div class="bg-white border-b border-slate-200 sticky top-0 z-40 shadow-sm">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
        <div class="flex items-center gap-3">
          <div class="p-2 bg-primary-100 rounded-lg">
            <svg class="w-6 h-6 text-primary-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
            </svg>
          </div>
          <div>
            <h1 class="text-2xl font-bold text-slate-900">Convocatorias y Concesiones</h1>
            <p class="text-sm text-slate-600">Busca y explora todas las convocatorias y concesiones de subvenciones</p>
          </div>
        </div>
      </div>
    </div>

    <!-- Main Content -->
    <main class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <!-- Filter Section -->
      <ConvocatoriaFilter @filter="handleFilter" />

      <!-- List Section -->
      <ConvocatoriaList 
        :filters="filters"
        @select-concesion="handleSelectConcesion"
      />
    </main>

    <!-- Detail Modal -->
    <ConvocatoriaDetail
      :concesion="selectedConcesion"
      @close="handleCloseConcesionDetail"
      @view-beneficiary-grants="handleViewBeneficiaryGrants"
    />

    <!-- Beneficiary Grants Modal -->
    <BeneficiaryGrants
      :beneficiario="selectedBeneficiario"
      @close="handleCloseBeneficiaryGrants"
    />
  </div>
</template>




