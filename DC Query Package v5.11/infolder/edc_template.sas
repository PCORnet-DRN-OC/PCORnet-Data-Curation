*******************************************************************************
* FILENAME: edc_template.sas
*    STUDY: PCORNET
*  PURPOSE: Create RTF styles definition for ODS output files.
*    INPUT: None
*   OUTPUT: work.template.sas7bcat
*
* ASSUMPTIONS: None.
*
* REVISION HISTORY:
******************************************************************************;
ods path sashelp.tmplmst(read) work.templat(write);

proc template;
    define style PCORNET_DCTL / store=work.templat;
    parent=styles.rtf;

    replace fonts /
        'TitleFont'           = ("Times",12pt)
        'TitleFont2'          = ("Times",10pt)        
        'StrongFont'          = ("Times",10pt)
        'EmphasisFont'        = ("Times",10pt)
        'FixedEmphasisFont'   = ("Courier New,Courier",9pt)
        'FixedStrongFont'     = ("Courier New,Courier",9pt)
        'FixedHeadingFont'    = ("Courier New,Courier",9pt)
        'BatchFixedFont'      = ("SAS Monospace,Courier New,Courier",9pt)
        'FixedFont'           = ("Courier New,Courier",9pt)
        'headingEmphasisFont' = ("Times",10pt)
        'headingFont'         = ("Times",10pt)
        'docFont'             = ("Times",9pt)
        'FootnoteFont'        = ("Times",9pt);

        style table from table / rules=groups
                                 frame=hsides
                                 cellspacing=0pt
                                 cellpadding=2pt
                                 borderwidth=2pt;

        style PageNo from PageNo / font_size=0.1pt
                                   background=white
                                   foreground=white;

        style BodyDate from BodyDate / font_size=0.1pt
                                       background=white
                                       foreground=white;

        style Header from Header / protectspecialchars=off 
                                   background=_undef_
                                   borderwidth=0.25pt
                                   frame=below
                                   rules=groups;

        style SystemFooter from TitlesAndFooters / protectspecialchars=off
                                                   font=Fonts('FootnoteFont');

        style systemtitle from systemtitle / protectspecialchars=off;
        
        style Data from Cell / protectspecialchars=off;    

        replace Body from Document /
                       bottommargin=0.40in  /*1.01 from footnote to edge*/
                       topmargin   =0.90in  /*1.18 from 1st header title to edge*/
                       rightmargin =0.75in
                       leftmargin  =0.75in;
    end;
    

    *** create modified style template for excel spreadsheets ***;
    define style work.XLsansPrinter / store=work.templat;
    parent = styles.sansPrinter;

    /*  Redefine characteristics of some of the standard style elements  */

        /* center ALL headers */
        style header from header / just = center;

        /* center ALL footers */
        style footer from footer / just = center;

        /* center ALL row headers */
        style rowheader from rowheader / just = center;

        /* vertically top allign ALL rows */
        style data from data / vjust=t;

        /* custom format to center designated columns */
        style cdata from data / just=c;

        /* custom format to left align designated columns */
        style ldata from data / just=l;

    end;

run;
