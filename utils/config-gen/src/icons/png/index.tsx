import React from 'react';

import am from './am.png';
import android from './android.png';
import emptyDestinations from './emptyDestination.png';
import ga from './ga.png';
import mp from './mp.png';
import fb from './fb.png';
import adj from './adj.png';
import hs from './hs.png';
import ios from './ios.png';
import s3 from './s3.png';
import minio from './minio.png';
import rs from './rs.png';
import af from './af.png';
import logo from './logo-rudder.png';
import graph from './graph.png';
import no_data from './noDataBubble.png';
import mc from './mc.png';
import transformations from './transformation_teaser.png';

const Img = (props: any) => {
  switch (props.name.toLowerCase()) {
    case 'android':
      return <img src={android} alt="Android" className="m-auto" />;
    case 'ios':
      return <img src={ios} alt="iOS" className="m-auto" />;
    case 'ga':
      return <img src={ga} alt="Google Analytics" className="m-auto" />;
    case 'am':
      return <img src={am} alt="Amplitude" className="m-auto" />;
    case 'mp':
      return <img src={mp} alt="Mixpanel" className="m-auto" />;
    case 'fb':
      return <img src={fb} alt="Facebook" className="m-auto" />;
    case 'adj':
      return <img src={adj} alt="Adjust" className="m-auto" />;
    case 'hs':
      return <img src={hs} alt="Hubspot" className="m-auto" />;
    case 's3':
      return <img src={s3} alt="Amazon S3" className="m-auto" />;
    case 'minio':
      return <img src={minio} alt="MinIO" className="m-auto" />;
    case 'rs':
      return <img src={rs} alt="Redshift" className="m-auto" />;
    case 'af':
      return <img src={af} alt="Redshift" className="m-auto" />;
    case 'mc':
      return <img src={mc} alt="Mailchimp" className="m-auto" />;
    case 'logo':
      return <img src={logo} alt="Rudder logo" className="m-auto" />;
    case 'ed':
      return (
        <img src={emptyDestinations} alt="No destinations" className="m-auto" />
      );
    case 'graph':
      return <img src={graph} alt="No data" className="m-auto" />;
    case 'no_data':
      return <img src={no_data} alt="No data" className="m-auto" />;
    case 'transformations':
      return (
        <img
          src={transformations}
          alt="Transformations"
          className="m-auto"
        ></img>
      );
    default:
      break;
  }
  return <img alt="Logo" />;
};

export default Img;
