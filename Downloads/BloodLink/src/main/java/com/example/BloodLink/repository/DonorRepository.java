package com.example.bloodlink.repository;

import com.example.bloodlink.entity.Donor;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DonorRepository extends JpaRepository<Donor, Long> {
}