package io.jay.batchprocessing.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "TB_CUSTOMER")
public class Customer {
    @Id
    private Integer id;
    private String firstName;
    private String lastName;
    private String email;
    private String gender;
    private String contactNumber;
    private String country;

    @Column(name = "DATE_OF_BIRTH")
    private String dob;

    private Timestamp lastUpdated;
}
